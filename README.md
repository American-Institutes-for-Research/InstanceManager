# InstanceManager

This tool is used to simplify working with AWS EC2 instances. It allows creating one or more instances, connecting to instances, sending terminal commands to instances, uploading files to instances, and downloading files from instances. 

In order to create instances, you will need to have a EC2 key pair `.pem` file or you need your AWS keys stored as environment variables. Read more about it [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html).

### Create instances
```python
# Instantiate InstanceManager object
manager = InstanceManager('MyKeyPair', environment_configuration=False, instance_num=1, 
                          instance_type='t2.micro', image_id='ami-06a75cf9d3bbf4cd9', username='ubuntu', 
                          home_directory='/home/ubuntu/', security_group_ids=None)

# Create an instance
instances = manager.create_instances(wait_for_running=True)
instance = instances[0]
```

### Connect to instances and execute terminal commands
```python
# Create SSH connection to instance
manager.connect_to_instances()

# List files
manager.execute_command('ls')

# Install dependencies on instance
manager.execute_command('pip install pandas')
```

### Upload files to instances
```python
# Upload data to instance
manager.upload_file_to_instance('input_data.csv', 'input_data.csv')

# Upload an analysis file to instance
manager.upload_file_to_instance('my_algorithm.py', 'my_algorithm.py')
```

### Run code on instances and download results
```python
# Execute analysis script that was uploaded
manager.execute_command('python my_algorithm.py input_data.csv output_data.csv')

# Download output files
manager.download_file_from_instance('output_data.csv', 'output_data.csv', instance)
```

### VERY IMPORTANT: Don't forget to terminate instances when done
```python
# Terminate instances
manager.terminate_instances()
```
