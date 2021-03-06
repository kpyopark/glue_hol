# 사전 준비
실습은 us-west-2 (오레곤) 리전을 사용합니다. 실습을 시작하기에 앞서 해당 리전을 선택합니다.

# Glue 용 IAM 생성
  
 * AWS Management 콘솔에 로그인한 다음 https://console.aws.amazon.com/iam/에서 IAM 콘솔을 엽니다.

 * 왼쪽 탐색 창에서 [Roles(역할)]를 선택합니다.

 * [Create role]을 선택합니다.
 
 * 이름은 임의로 작성합니다. ex: GlueRole

 * 역할 유형의 경우, [AWS Service(AWS 서비스)]를 선택하고 [Glue]를 찾아 선택한 다음 [Next: Permissions(다음: 권한)]를 선택합니다.

 * Attach permissions policy(권한 정책 추가) 페이지에서 AWSGlueServiceRole과 Amazon S3 리소스로의 액세스를 위한 AWS 관리형 정책 AmazonS3FullAccess를 선택합니다. 그런 다음 [Next: Review]를 선택합니다. 
 
 * [Save]를 수행합니다. 


# 개발 Enpoint 활성화 

 * https://console.aws.amazon.com/glue/에서 AWS Glue 콘솔을 엽니다. IAM 권한 glue:GetDevEndpoints 및 glue:GetDevEndpoint가 있는 사용자로 로그인하십시오.

 * 탐색 창의 ETL 아래에서 개발 엔드포인트를 선택합니다.

 * 개발 엔드포인트 페이지에서 Add Endpoint를 클릭합니다. 
 
 * 이름은 편하게 작성하면 됩니다. ex: Glue_test

 * IAM은 위에서 생성한 IAM Role을 선택하십시요. 
 
 * Networking / SSH Key Upload는 건너 뜁니다.
 
 * Finish 를 수행하면 생성이 됩니다. 


# Crawler를 이용한 메타데이터 생성
AWS Glue 콘솔에서 크롤러 작업의 단계에 따라 s3://awsglue-datasets/examples/us-legislators/all 데이터 세트를 이라는 이름의 데이터베이스에 크롤할 수 있는 새로운 크롤러를 생성합니다. 이 예제 데이터는 이 퍼블릭 Amazon S3 버킷에 이미 존재합니다.

 * AWS Management 콘솔에 로그인한 후 콘솔을 엽니다.
 
 * Glue 서비스로 이동합니다. 
 
 * Crawler 메뉴에서 [Add Crawler]를 선택합니다. 

 * Crawler 이름에 legislators_crawler 를 입력합니다. 다음 창에서 Data stores 를 선택합니다.
 
 * S3를 선택하고, [Specified path in another account]를 선택한 후, s3://awsglue-datasets/examples/us-legislators/all 를 입력합니다. 

 * 추가로 Data store를 지정하지 않고 생성합니다. 

 * IAM Role에는 Default값인 'Create an IAM Role'이 아니라, 앞에서 생성 Role을 이용해야 하기 때문에, 'Choose an existing IAM role'을 선택하고, 앞에서 생성했던 Glue-Role을 선택합니다. 
 
 * Frequency는 [Run on Demand] 로 선택합니다. 

 * Add Database를 선택하여, 신규 메타 스토어를 생성합니다. 이름은 legislators 로 지정하면됩니다. 별도의 Prefix는 추가하지 않습니다. 이후 Next를 눌러 Crawler 생성을 완료합니다. 

 * 신규로 추가된 Crawler을 수행합니다. 

 * 이후 6개의 테이블이 아래와 같이 생성되었음을 확인할 수 있습니다. (약 1분)

    persons_json

    memberships_json

    organizations_json

    events_json

    areas_json

    countries_r_json

 * 제정자와 제정자 기록을 포함한 테이블 반정규화된 컬렉션입니다.

## Sagemaker Notebook 을 Developer Endpoint에서 기동하기
위에서 생성한 개발자 Endpoint를 이용하기 위하여, Glue Context가 들어있는 초기 Header 부분 Script를 Notebook 제일 상단에 적어 놓습니다. 

 * Console Glue 메뉴에서 Dev Endpoint메뉴를 선택하고, 위에서 생성한 Dev Endpoint Cluster를 선택합니다. 

 * [Create Sagemaker Notebook]을 선택합니다. 

 * Notebook Name 값은 임의로 지정합니다. ex. join-test-notebook

 * Attach to Development Endpoint에 위에서 생성한 Developer Endpoint Cluster 이름을 적습니다. ex. Glue_test

 * IAM Role은 [Create an IAM role] 를 선택합니다. 이름에는 glue-test를 입력합니다. 

 * VPC는 No VPC를 선택합니다. Encryption Key는 입력하지 않습니다. 

 * 기동을 하는데 약 5분 정도 시간이 걸립니다. (Ready로 상태가 변경됨)

 * 이후, [Open Notebook] 을 이용하여 Notebook을 기동합니다. 

## Data Load Test를 위한 Redshift Cluster 생성

 * Console redshift 메뉴로 이동합니다. 

 * [Create cluster] 를 선택합니다. 

 * DC2 3 Node를 선택합니다. 

 * 이름을 redshift-glue-test 로 기입합니다. 

 * Master User 정도는 그대로 두고, Master password를 입력합니다. (기억해 두셔야 합니다.)

 * IAM에 'AWSServiceRoleForRedshift'를 선택합니다

 * 생성합니다. Default로 Database Name은 dev 로 생성되며, Security Group 및, Public Accessibility가 False로 설정되어 있습니다. 이럴 경우, 외부에서 접근이 불가능하여 no VPC Dev Endpoint에서는 접근이 어렵습니다. 

 * 생성이 완료되면, 해당 Cluster를 선택하고, [Properties] 탭으로 이동합니다. 

 * 제일 하단에 있는 Public Accessible 항목을 Yes로 수정합니다. EIP는 별도 선택이 없으면, 자동으로 할당합니다. 

 * 수정을 한 이후 잠시 기다리면 (2분), 해당 항목이 Yes로 바뀐 것을 확인할 수 있습니다. 이제 Security Group를 수정해야 합니다. 

 * 마찬가지로 [Properties] 탭 하단에 있는 할당되어 있는 Security Group을 선택합니다. 

 * 별도의 창이 뜨면, 해당 Security Group을 선택하고, 하단에 있는 [Inbound] 탭을 선택합니다. 

 * [Edit] 버튼을 선택하고, 창이 뜨면, Type을 [Redshift] 항목으로 수정하고, [Source]를 [Anywhere] 로 변경하십시요. 

 * 추가적으로 Type을 [All TCP]로 수정하고, [Source]를 동일한 Security Group으로 지정합니다. 이는 향후 JDBC Connection을 연결하기 위해서 ENI를 해당 VPC에 추가할때, ENI에 Redshift에 부여된 SG를 부여하기 때문입니다.

 * [Save] 버튼을 선택하면 설정이 모두 완료되었습니다. 

 * Redshift의 [Properties] 탭을 선택하고, endpoint를 복사하거나, 별도로 적어 두시기 바랍니다. 이후 Glue Connection 생성시 활용됩니다. 

## Data Load Test를 위한 VPC S3 Endpoint 생성

 * Console상에서 VPC 메뉴를 선택합니다. 
 
 * 왼쪽에 있는 Endpoint 메뉴를 선택합니다. 

 * [Create Endpoint] 를 선택합니다. 

 * Service Name에 s3를 입력하고 검색을 하면, com.amazonaws.us-west-2.s3 가 검색됩니다. 

 * VPC를 Default VPC를 선택하고, [Create Endpoint]를 선택합니다. 

## Data Load Test를 위한 Glue Connection 생성

 * Console에서 Glue 메뉴로 이동합니다. 

 * 왼쪽에 있는 Databases / Connections 메뉴를 선택합니다. 

 * [Add Connection] 을 선택합니다. 

 * 이름은 반드시 redshift-glue-test 로 입력합니다. Connection Type은 JDBC를 선택합니다. 

 * jdbc 항목에 위에서 생성한 Redshift의 Endpoint를 이용하여 아래와 같이 적어 줍니다. 
   jdbc:redshift://<your redshift cluster endpoint>:5439/dev

 * username에는 awsuser, Password에는 앞에서 입력한 password를 넣어 주세요. 

 * VPC는 Default VPC를 적습니다. 

 * Subnet은 한개를 선택해 주세요. 

 * Security Group을 Default Security Group를 선택하십시요.

 * 생성을 완료합니다. 

 * 원래 화면에서, 생성된 Connection을 선택하고, [Test Connection]을 수행합니다. (시간이 2분 정도 걸림) 상단에
   redshift-glue-test connected successfully to your instance
   가 나오면 정상입니다. 

## Notebook에서 수행 Script 작성 (Optional)

 * 오른쪽 상단에 있는 [new] 를 선택하고, SparkMagic(PySpark)을 선택합니다. 

 * 신규 창이 뜨면서 노트북 Editor가 보이면, 제일 상단에 있는 Note Name을 Glue-Test(임의로 수정 가능)로 입력합니다. 

 * 아래 스크립트는 GlueContext를 사용하기 위한 기본 내용을 포함하고 있습니다. Copy and Paste로 Script 제일 앞에 붙여 주십시요.

``` python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
```

 * 이후 필요한 작업을 수행하면 됩니다. 

## Notebook에서 Demo Script를 Load

 * 본 Project에 포함되어 있는 [Joining, Filtering, Loading Relational Data with Glue - In KR.ipynb] 파일을 Notebook 상단오른쪽에 있는 [Upload] 버튼을 클릭하여 로드해 주세요. 

 * 이후에는 해당 Example을 따라가면서 수행을 진행해 보십시요.
