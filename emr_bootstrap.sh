aws emr create-cluster --applications Name=Hadoop Name=Spark --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-58903d20","EmrManagedSlaveSecurityGroup":"sg-0559641760222403e","EmrManagedMasterSecurityGroup":"sg-01e984deb4a4a0928"}' --release-label emr-5.33.0 --log-uri 's3n://aws-logs-240635111244-us-west-2/elasticmapreduce/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster", "s3://udacity-de-python-etl-script/etl.py" ],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{}}]' --auto-terminate --service-role EMR_DefaultRole --enable-debugging --name 'Data Lake Process' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2