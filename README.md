# glue_hol
It came from the Hyobin's documents.

# 사전 준비
실습은 us-west-2 (오레곤) 리전을 사용합니다. 실습을 시작하기에 앞서 해당 리전을 선택합니다.

# EC2 키 페어 생성
이후 생성할 EC2에 SSH 접속을 하기 위해서는 키가 필요합니다. 이미 us-west-2리전에 사용 중인 키가 있다면 건너 뜁니다.

1.AWS Management Console에서 EC2 서비스로 이동합니다.
2.좌측 메뉴에서 [키 페어] 메뉴를 클릭한 뒤 [키 페어 생성] 을 클릭합니다.
3.[키 페어 이름] 을 입력한 뒤 [생성] 버튼을 클릭하여 완료합니다.
4.pem 파일 다운로드가 잘 되었는지 확인합니다.

CloudFormation 스택 생성 및 확인
실습에 사용할 EC2, VPC, IAM, Elasticsearch, Redshift, RDS for Aurora, DynamoDB, Lambda 등 대부분의 서비스를 CloudFormation을 통해 프로비저닝합니다.

1.	AWS Management Console에서 CloudFormation 서비스로 이동합니다.
2.	[스택 생성] 버튼을 클릭합니다. [Amazon S3 템플릿 URL 지정] 옵션을 선택하고 다음 URL을 입력합니다. https://s3.ap-northeast-2.amazonaws.com/icn-datalab/code/datalab-cloudformation-template_final.json [다음] 을 선택합니다. 
3.	[스택 이름] 에 적절한 이름과 [KeyName] 부분에 EC2 키 페어를 선택합니다. [다음] 을 선택하여 진행합니다.
4.	옵션 화면에서도 [다음]을 선택한 뒤 검토 화면에서 [AWS CloudFormation에서 사용자 정의 이름으로 IAM 리소스를 생성할 수 있음을 승인합니다.] 옵션을 체크한 뒤 [생성] 을 클릭하여 스택 생성을 완료합니다. 
5.	스택 생성에는 약 40분 소요됩니다.

