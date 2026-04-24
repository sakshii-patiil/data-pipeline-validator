pipeline {
    agent any

    triggers {
        cron('H 2 * * *')  // Nightly at ~2 AM
    }

    environment {
        SF_USER     = credentials('snowflake-user')
        SF_PASSWORD = credentials('snowflake-password')
        SF_ACCOUNT  = credentials('snowflake-account')
        SF_DATABASE = 'ANALYTICS_DB'
        SF_SCHEMA   = 'PUBLIC'
        SF_WAREHOUSE= 'COMPUTE_WH'
        SF_ROLE     = 'SYSADMIN'
    }

    stages {
        stage('Setup') {
            steps {
                sh 'python3 -m venv venv'
                sh './venv/bin/pip install -r requirements.txt -q'
            }
        }

        stage('Schema Contract Tests') {
            steps {
                sh '''
                    ./venv/bin/pytest tests/test_schema_contracts.py \
                        --alluredir=allure-results/schema \
                        -v --tb=short
                '''
            }
        }

        stage('ETL Pipeline Tests') {
            steps {
                sh '''
                    ./venv/bin/pytest tests/test_etl_pipeline.py \
                        --alluredir=allure-results/etl \
                        -v --tb=short
                '''
            }
        }

        stage('Allure Report') {
            steps {
                allure([
                    includeProperties: false,
                    jdk: '',
                    reportBuildPolicy: 'ALWAYS',
                    results: [[path: 'allure-results']]
                ])
            }
        }
    }

    post {
        failure {
            mail(
                to: 'spati164@asu.edu',
                subject: "PIPELINE FAILED: Data Quality Gate [${env.BUILD_NUMBER}]",
                body: """
                    Nightly data pipeline validation FAILED.
                    Build: ${env.BUILD_URL}
                    Stage: ${env.STAGE_NAME}

                    Check Allure report for schema drift, NULL violations, or row-count mismatches.
                """
            )
        }
        always {
            cleanWs()
        }
    }
}
