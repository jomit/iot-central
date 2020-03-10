# Setup
- Download v1.0.0  Ubuntu release binary from [here](https://github.com/mtconnect/open62541_ua_server/releases)

- `docker build -t opcua-mtserver .`

- `docker run -it -p 4840:4840 --name="mymtserver" opcua-mtserver`

# Local Testing on Windows

- & 'C:\Program Files (x86)\MTConnect OPCUA Server\bin\opcua-MTServer.exe' https://smstestbed.nist.gov/vds/GFAgie01 60

# Docker Helper Commands 

- `docker rm $(docker ps -a -q)`
- `docker rmi $(docker images -q)`
- `docker rm $(docker ps -a -q) -f "dangling=true" -q)`
- `docker rmi $(docker images -f "dangling=true" -q)`
- Install docker on Ubuntu: https://docs.docker.com/install/linux/docker-ce/ubuntu/
- Find dependencies: `ldd opcua-MTServer`