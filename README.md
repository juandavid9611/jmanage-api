# jmanage-api
Management Tool

Steps to deploy:
1. Perform needed changes
2. pip freeze > requirements.txt (if changes on pip dependencies)
3. ./package_for_lambda.sh
4. Run cdk deploy on infra project. This assume that api project and infra project are under the same folder