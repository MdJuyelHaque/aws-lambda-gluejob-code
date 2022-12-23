# GLUE-JOB ENVIRONMENT SETUP #

This README contains all the necessary steps to setup environment for Glue-Job.

### Follow the below given steps to setup environment for Glue-Job to run the python shell script ###

* Install Docker for Linux, Windows, or macOS on your computer.
* Create a requirements.txt file with required Python modules and their versions.
* Create a environment.sh to setup environment.

### Create a wheelhouse using the following Docker command: ###

* docker run -v "$PWD":/tmp amazonlinux:latest /bin/bash -c "cd /tmp;sh environment.sh"

### Copy the wheelhouse directory into the S3 bucket using following code: ###

* S3_BUCKET="BUCKET_NAME"
* aws s3 cp wheelhouse/ "s3://$S3_BUCKET/wheelhouse/" --recursive --profile default













