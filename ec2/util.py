import boto3


def create_instance(tag_name, params):
  print("Creating instance", tag_name)
  ec2 = boto3.resource("ec2")
  instances = ec2.create_instances(
    ImageId=params["image_id"],
    InstanceType=params["instance"],
    KeyName=params["key"],
    MinCount=1,
    MaxCount=1,
    NetworkInterfaces=[{
      "SubnetId": params["subnet"],
      "DeviceIndex": 0,
      "Groups": [params["security"]]
    }],
    TagSpecifications=[{
      "ResourceType": "instance",
      "Tags": [{
        "Key": "Name",
        "Value": tag_name,
      }]
    }]
  )
  assert(len(instances) == 1)
  instance = instances[0]
  instance.wait_until_running()
  return instance
