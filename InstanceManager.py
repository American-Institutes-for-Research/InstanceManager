import os
import boto3
import paramiko
import time
import atexit
import botocore.exceptions
import io


class InstanceManager:
    def __init__(self, key_name, key_file=None, environment_configuration=False, instance_num=1, instance_type='c5.large', image_id='ami-0a47106e391391252',
                 username='ubuntu', home_directory='/home/ubuntu/', security_group_ids=None):
        """
        Initiate InstanceManager() object

        :param key_name: (str) Name of the key pair created.
        :param key_file: (str) Location of the key pair .pem file.
        :param environment_configuration: (bool) Whether AWS access key and region information is stored as environment
            variables. If false, then the information should be set up using the AWS CLI configuration tool. If true,
            the environment variable names should be 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', and
            'AWS_DEFAULT_REGION'
        :param instance_num: (int) Number of instances to create
        :param instance_type: (str) The name of the EC2 instance type found at
            https://aws.amazon.com/ec2/instance-types/
        :param image_id: (str) The ID of the Amazon Machine Image (AMI) that instances will launch with
        :param username: (str) The username of the AMI
        :param home_directory: (str) The home directory of the AMI
        :param security_group_ids: (list of str) The ID of user created security groups that instances will use. If none
            is given, then a security group will be created to allow all incoming traffic.
        """

        # A list of instances
        self.instances = []

        # A dictionary of SSH connections to each instance, where keys are instance ID and values are SSH clients
        self.ssh_clients = {}

        # If environment_configuration is False, then search the environment variables to get access key info
        # Otherwise, boto3 will search the .aws directory in the local home directory for credential files
        self.environment_configuration = environment_configuration
        if environment_configuration:
            self.ec2 = boto3.resource('ec2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                      region_name=os.environ['AWS_DEFAULT_REGION'])
        else:
            self.ec2 = boto3.resource('ec2')

        self.key_name = key_name
        self.key_file = key_file
        self.security_group_ids = security_group_ids
        self.instance_num = instance_num
        self.instance_type = instance_type
        self.image_id = image_id
        self.username = username
        self.home_directory = home_directory
        self.security_group_ids = security_group_ids
        self.security_group_created = False

        # When script ends, run cleanup code
        atexit.register(self.cleanup)

    def cleanup(self):
        """
        Terminates all instances and deletes all security groups that were created by the program

        :return: None
        """
        self.terminate_instances(wait_until_terminated=self.security_group_created)

        if self.security_group_created:
            self.delete_security_group()

    def __parse_instances(self, instances):
        """
        Check to make sure provided instances exist in the self.instances array

        :param instances: List of AWS instances
        :return: List of AWS instances unless an instance proved in the parameter does not exist
        """
        if instances is None:
            return self.instances

        # If only one instance is provided not in a list, put it in a list
        try:
            iter(instances)
        except TypeError:
            instances = [instances]

        # Loop through provided instances and check to see if each one is in self.instances
        for instance in instances:
            if instance not in self.instances:
                print('TypeError: Instances provided in parameters do not exist in InstanceManager object')
                raise TypeError

        return instances

    def create_security_group(self):
        """
        Creates a security group with unlimited ingress on ports 22 and 80

        :return: None
        """
        try:
            # Create security group
            security_group = self.ec2.create_security_group(GroupName='IMPAQ_HPC_TM',
                                                            Description='Used for HPC topic modeling')

            # Set permissions for security group
            # Allow all IP ranges to and from ports 22 and 80
            security_group.authorize_ingress(
                IpPermissions=[
                    {'IpProtocol': 'tcp',
                     'FromPort': 80,
                     'ToPort': 80,
                     'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
                     'Ipv6Ranges': [{'CidrIpv6': '::/0'}]},
                    {'IpProtocol': 'tcp',
                     'FromPort': 22,
                     'ToPort': 22,
                     'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
                     'Ipv6Ranges': [{'CidrIpv6': '::/0'}]}
                ])

            # Keep track of this security group
            self.security_group_ids = [security_group.id]

            # Keep track that a security group was created to delete it after instance is terminated
            self.security_group_created = True
        except botocore.exceptions.ClientError as e:
            # If this error occurs, the security group has already been created

            print('Security group already created')
            if 'InvalidGroup.Duplicate' in str(e):
                response = boto3.client('ec2').describe_security_groups(GroupNames=['IMPAQ_HPC_TM'])
                self.security_group_ids = [response['SecurityGroups'][0]['GroupId']]
            else:
                raise

    def delete_security_group(self):
        """
        Deletes the security groups in self.security_groups_ids

        :return: None
        """

        # Loop through security group IDs and delete each one
        for security_group_id in self.security_group_ids:
            try:
                if self.environment_configuration:
                    boto3.client('ec2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region_name=os.environ['AWS_DEFAULT_REGION']).delete_security_group(GroupId=security_group_id)
                else:
                    boto3.client('ec2').delete_security_group(GroupId=security_group_id)
                print('Security group {} deleted'.format(security_group_id))
            except botocore.exceptions.ClientError as e:
                print(e)

    def create_instances(self, wait_for_running=True):
        """
        Create AWS instances

        :param wait_for_running: (boolean) If True, block until instances enter "Running" state
        :return: AWS instances
        """

        # If a security group hasn't been created yet, create one
        if self.security_group_ids is None:
            self.create_security_group()

        # Create instances
        self.instances = self.ec2.create_instances(
            ImageId=self.image_id,
            InstanceType=self.instance_type,
            MaxCount=self.instance_num,
            MinCount=self.instance_num,
            SecurityGroupIds=self.security_group_ids,
            KeyName=self.key_name
        )

        # Wait for the instances to start running and load their information
        if wait_for_running:
            self.load_instances()

        return self.instances

    def load_instances(self):
        """
        Load instances to get their updated information

        :return: None
        """
        for instance in self.instances:
            # Instances should be running before being loaded or they will be missing a lot of information
            instance.wait_until_running()
            instance.load()

    def connect_to_instances(self, instances=None, max_attempts=10, password=None):
        """
        Create SSH connections of AWS instances

        :param instances: AWS instances
        :param max_attempts: Number of times to attempt connecting before giving up
        :return: None
        """
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        for instance in instances:
            for connection_attempts in range(1, max_attempts + 1):
                try:
                    # Create SSH client
                    client = paramiko.SSHClient()

                    # Set client policy
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                    # Use .pem file to create a key
                    if self.key_file is None:
                        key = paramiko.RSAKey.from_private_key(io.StringIO(str(os.environ.get("AWS_PRIVATE_KEY")).replace('\\n', '\n')))
                    else:
                        key = paramiko.RSAKey.from_private_key_file(self.key_file)

                    # Connect client to instance
                    client.connect(hostname=instance.public_ip_address, username=self.username, pkey=key, password=password)

                    # If already connected to instance, close previous connection
                    if instance.id in self.ssh_clients:
                        self.ssh_clients[instance.id].close()

                    # Keep track of SSH clients currently connected to instance
                    self.ssh_clients[instance.id] = client
                    break
                except TimeoutError:
                    # Sometimes connection attempt times out
                    if connection_attempts == max_attempts:
                        raise

                    print('Connection attempt #{} for IP address {} timed out. Trying again...'
                          .format(connection_attempts, instance.public_ip_address))
                    time.sleep(10)
                except paramiko.ssh_exception.NoValidConnectionsError:
                    # This error also occurs sometimes. Just need to retry connecting
                    if connection_attempts == max_attempts:
                        raise

                    print('Connection attempt #{} for IP address {} failed. Trying again...'
                          .format(connection_attempts, instance.public_ip_address))
                    time.sleep(10)
                except paramiko.ssh_exception.AuthenticationException:
                    if connection_attempts == max_attempts:
                        raise

                    print('Connection attempt #{} for IP address {} failed authentication. Trying again...'
                          .format(connection_attempts, instance.public_ip_address))
                    time.sleep(10)


    def terminate_instances(self, instances=None, wait_until_terminated=False):
        """
        Terminate instances

        :param instances: AWS Instances
        :param wait_until_terminated: (bool) If True, block until instances are terminated
        :return: None
        """
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        # Terminate instances
        for instance in instances:
            print('Terminating instance', instance.id)
            instance.terminate()

        # Wait for instances to be terminated
        if wait_until_terminated:
            for instance in instances:
                instance.wait_until_terminated()
                print('Instance', instance.id, 'terminated')

        # Close SSH connections to instances that are now terminated
        self.close_instance_connections(instances, suppress_warning=True)

    def start_instances(self, instances=None, wait_until_running=True):
        """
        Start instances

        :param instances: AWS Instances
        :param wait_until_running: (boolean) If True, block until instances enter "Running" state
        :return:
        """
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        # Start instance
        for instance in instances:
            print('Starting instance', instance.id)
            instance.start()

        # Wait for instances to enter running state
        if wait_until_running:
            for instance in instances:
                instance.wait_until_running()
                print('Instance', instance.id, 'running')

    def stop_instances(self, instances=None, wait_until_stopped=False):
        """
        Stop instances

        :param instances: AWS Instance
        :param wait_until_stopped: (boolean) If True, block until instances enter "Stopped" state
        :return: None
        """
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        # Stop instances
        for instance in instances:
            print('Terminating instance', instance.id)
            instance.stop()

        # Wait for instances to stop
        if wait_until_stopped:
            for instance in instances:
                instance.wait_until_stopped()
                print('Instance', instance.id, 'stopped')

        # Close SSH connections to closed instances
        self.close_instance_connections(instances, suppress_warning=True)

    def close_instance_connections(self, instances=None, suppress_warning=False):
        """
        Close SSH clients that are connected to instances

        :param instances: AWS Instances
        :param suppress_warning: (bool) If True, no warnings for trying to close connections that don't exist
        :return: None
        """
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        for instance in instances:
            try:
                # Close connection for SSH client associated with each instance
                self.ssh_clients[instance.id].close()
            except KeyError:
                if not suppress_warning:
                    print('Instance {} does not have an open SSH connection'.format(instance.id))

    def upload_file_to_instance(self, source_file, destination_file, instances=None):
        """
        Uploads a file to one or more instances

        :param source_file: (str) File path for file that will be uploaded
        :param destination_file: (str) Name of file in the instance
        :param instances: AWS instances
        :return: None
        """
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        for instance in instances:
            client = self.ssh_clients[instance.id]

            # Open SFTP connection
            sftp = client.open_sftp()

            # Upload file
            sftp.put(source_file, os.path.join(self.home_directory, destination_file))

            # Close connection
            sftp.close()

    def download_file_from_instance(self, source_file, destination_file, instance):
        """
        Download file from instance to local machine

        :param source_file: (str) File path of the file in the instance
        :param destination_file: (str) File path that will be used on local machine
        :param instance: AWS instance
        :return: None
        """
        try:
            client = self.ssh_clients[instance.id]

            # Open SFTP connection
            sftp = client.open_sftp()

            # Download file
            sftp.get(source_file, destination_file)

            # Close connection
            sftp.close()
        except KeyError:
            print('KeyError: That instance does not have an open connection')
            raise

    def execute_command(self, command, instances=None):
        """
        Execute terminal command on instances

        :param command: (str) Terminal command
        :param instances: AWS Instances
        :return: None
        """
        print('Executing:', command)
        # Make sure instances given in parameters are in the InstanceManager object
        instances = self.__parse_instances(instances)

        for instance in instances:
            client = self.ssh_clients[instance.id]

            # Execute command
            stdin, stdout, stderr = client.exec_command(command)

            # Get exit status of command
            exit_status = stdout.channel.recv_exit_status()

            if exit_status == 0:
                # If exit status is 0, then there were no errors
                # Print output of command
                for line in stdout.readlines():
                    print(line, end='')
            else:
                # Print exit status is not 0, there were errors
                # Print error
                for line in stderr.readlines():
                    print(line, end='')

    def download_file_from_url(self, url, instances=None):
        """
        Download a file from a URL to instances

        :param url: (str) URL of file to be downloaded
        :param instances: AWS instances
        :return: None
        """
        # Downloading a file is just using the command wget
        command = 'wget {}'.format(url)

        # Execute command on instances
        self.execute_command(command, instances)
