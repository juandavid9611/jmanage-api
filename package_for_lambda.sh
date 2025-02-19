#!/bin/bash

# Exit if any command fails
set -eux pipefail

pip install -t lib -r requirements.txt
(cd lib; zip ../lambda_function.zip -r  .)
zip lambda_function.zip -u app.py
zip lambda_function.zip -u cognito_idp_actions.py
zip lambda_function.zip -u JWTBearer.py
zip lambda_function.zip -u auth.py

# Clean up
rm -rf lib

git add .
git commit -m "Update lambda function"
git push