import configparser
import json
import time

import boto3
from click import Option, UsageError, command, option


class MutuallyExclusiveOption(Option):
    """
    Implements mutually exclusive options for click module
    """

    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = help + (
                ' NOTE: This argument is mutually exclusive with '
                ' arguments: [' + ex_str + '].'
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(
                    self.name,
                    ', '.join(self.mutually_exclusive)
                )
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx,
            opts,
            args
        )


def create_iam_role(iam_client, DWH_IAM_ROLE_NAME):
    """Creates an IAM role allowing redshift cluster access AWS services

    Args:
        iam_client (IAM.Client): client to access IAM service
        DWH_IAM_ROLE_NAME (str): IAM role name

    Returns:
        dict: information about the specified role
    """
    try:
        print("Creating a new IAM Role: {}".format(DWH_IAM_ROLE_NAME))
        dwhRole = iam_client.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to \
                call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal':
                                    {'Service':
                                        'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
        print("Attaching Policy")
        iam_client.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']
        return dwhRole['Role']
    except Exception as e:
        print(e)


def delete_iam_role(iam_client, DWH_IAM_ROLE_NAME):
    """Deletes an IAM role

    Args:
        iam_client (IAM.Client): client to access IAM service
        DWH_IAM_ROLE_NAME (str): IAM role name
    """
    try:
        print("Deleting the IAM Role: {}".format(DWH_IAM_ROLE_NAME))
        iam_client.delete_role(DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)


def get_iam_role(iam_client, DWH_IAM_ROLE_NAME):
    """Gets an IAM role

    Args:
        iam_client (IAM.Client): client to access IAM service
        DWH_IAM_ROLE_NAME (str): IAM role name

    Returns:
        dict: information about the specified role
    """
    try:
        print("Get the IAM Role: {}".format(DWH_IAM_ROLE_NAME))
        return iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']
    except Exception as e:
        print(e)


def get_cluster_status(redshift_client, DWH_CLUSTER_IDENTIFIER):
    """Retrieves the redshift cluster status

    Args:
        redshift_client (Redshift.Client): client to access Redshift service
        DWH_CLUSTER_IDENTIFIER (str): cluster name

    Returns:
        str: status of the cluster or 'unknown'
    """
    try:
        print("Get Redshift cluster status: {}".format(DWH_CLUSTER_IDENTIFIER))
        myClusterProps = redshift_client.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        return myClusterProps['ClusterStatus']
    except redshift_client.exceptions.ClusterNotFoundFault:
        return 'unknown'


def create_redshift_cluster(redshift_client, roleArn, DWH_CLUSTER_TYPE,
                            DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                            DWH_CLUSTER_IDENTIFIER, DWH_DB_USER,
                            DWH_DB_PASSWORD):
    """Creates Redshift cluster

    Args:
        redshift_client (Redshift.Client): client to access Redshift service
        roleArn (str): ARN of the IAM role
        DWH_CLUSTER_TYPE (str): cluster type (single-node, multi-node)
        DWH_NODE_TYPE (str): EC2 type of the nodes (dc2.large)
        DWH_NUM_NODES (str): number of nodes (only for multi-node cluster)
        DWH_DB (str): data base name
        DWH_CLUSTER_IDENTIFIER (str): cluster name
        DWH_DB_USER (str): db user name
        DWH_DB_PASSWORD (str): db user password

    Returns:
        dict: response of the create cluster call
    """
    try:
        print("Create Redshift cluster: {}"
              .format(DWH_CLUSTER_IDENTIFIER))
        # HW
        hw_params = {'ClusterType': DWH_CLUSTER_TYPE}
        if (DWH_NODE_TYPE == "multi-node"):
            hw_params['NumberOfNodes'] = int(DWH_NUM_NODES)
        hw_params['NodeType'] = DWH_NODE_TYPE

        response = redshift_client.create_cluster(
            **hw_params,

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
        return response
    except Exception as e:
        print(e)


def delete_redshift_cluster(redshift_client, DWH_CLUSTER_IDENTIFIER):
    """Deletes redshift cluster

    Args:
        redshift_client (Redshift.Client): client to access Redshift service
        DWH_CLUSTER_IDENTIFIER (str): cluster name
    """
    try:
        print("Delete Redshift cluster: {}"
              .format(DWH_CLUSTER_IDENTIFIER))
        redshift_client.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)


@command()
@option('--create', '-c', cls=MutuallyExclusiveOption,
        help=u'Create redshift cluster.',
        mutually_exclusive=["delete", "status"],
        is_flag=True)
@option('--delete', '-d', cls=MutuallyExclusiveOption,
        help=u'Delete redshift cluster.',
        mutually_exclusive=["create", "status"],
        is_flag=True)
@option('--status', '-s', cls=MutuallyExclusiveOption,
        help=u'Get status of redshift cluster.',
        mutually_exclusive=["create", "delete"],
        is_flag=True)
def main(create, delete, status):

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    DWH_REGION = config.get("DWH", "DWH_REGION")

    iam_client = boto3.client('iam', aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET,
                              region_name=DWH_REGION
                              )

    redshift_client = boto3.client('redshift',
                                   region_name=DWH_REGION,
                                   aws_access_key_id=KEY,
                                   aws_secret_access_key=SECRET
                                   )

    if(status):

        print(get_cluster_status(redshift_client, DWH_CLUSTER_IDENTIFIER))

    if(create):

        dwhRole = get_iam_role(iam_client, DWH_IAM_ROLE_NAME)

        if(not dwhRole):
            dwhRole = create_iam_role(iam_client, DWH_IAM_ROLE_NAME)

        status = get_cluster_status(redshift_client, DWH_CLUSTER_IDENTIFIER)

        if(status == 'unknown'):
            create_redshift_cluster(redshift_client, dwhRole['Arn'],
                                    DWH_CLUSTER_TYPE, DWH_NODE_TYPE,
                                    DWH_NUM_NODES, DWH_DB,
                                    DWH_CLUSTER_IDENTIFIER,
                                    DWH_DB_USER, DWH_DB_PASSWORD)
            time.sleep(10)

            i = 10
            while i > 0:
                status = get_cluster_status(
                    redshift_client, DWH_CLUSTER_IDENTIFIER)
                if(status == 'available'):
                    break
                i -= 1
                time.sleep(10)

                print(status)

    if(delete):

        status = get_cluster_status(redshift_client, DWH_CLUSTER_IDENTIFIER)

        if(status == 'available'):
            delete_redshift_cluster(redshift_client, DWH_CLUSTER_IDENTIFIER)

            i = 10
            while i > 0:
                status = get_cluster_status(
                    redshift_client, DWH_CLUSTER_IDENTIFIER)
                if(status == 'unknown'):
                    break
                i -= 1
                time.sleep(10)

                print(status)


if __name__ == "__main__":
    main()
