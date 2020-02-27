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

 * Crawler 이름에 legislators_crawler를 입력합니다. 
 
 * 
 
 

새로운 크롤러를 실행한 다음 legislators 데이터베이스를 확인합니다.

크롤러는 다음 메타데이터 테이블을 생성합니다.

persons_json

memberships_json

organizations_json

events_json

areas_json

countries_r_json

제정자와 제정자 기록을 포함한 테이블 반정규화된 컬렉션입니다.


