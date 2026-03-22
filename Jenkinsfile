pipeline {

    agent { label 'Jenkins' }

    environment {
        BUILD_ID    = "${env.BUILD_NUMBER}"
        ENVIRONMENT = "${env.BRANCH_NAME}"    // auto-map branch → environment
    }

    stages {

        stage('Checkout') {
            steps {
                script {
                    echo "Starting Checkout Stage"
                    echo "Branch: ${env.BRANCH_NAME}"
                    echo "Build: ${env.BUILD_NUMBER}"
                    echo "Custom BUILD_ID: ${BUILD_ID}"
                    echo "Multibranch automatically checks out the branch."
                }
            }
        }

        stage('SonarQube Analysis') {
            when {
                expression { ENVIRONMENT != 'prod' }
            }
            environment {
                SONARQUBE_NAME = 'Sonar'
                SCANNER_TOOL   = 'SonarQube'
                SONARQUBE_URL  = 'http://cicdjobsvm.amd.com:9000'
            }
            steps {
                script {
                    def scannerHome = tool name: SCANNER_TOOL, type: 'hudson.plugins.sonar.SonarRunnerInstallation'
                    withSonarQubeEnv(SONARQUBE_NAME) {
                        sh """${scannerHome}/bin/sonar-scanner \
                            -Dsonar.projectName=cca-eia-api-${env.BRANCH_NAME} \
                            -Dsonar.projectBaseDir=${env.WORKSPACE} \
                            -Dsonar.projectKey=cca-eia-api-${env.BRANCH_NAME} \
                            -Dsonar.sourceEncoding=UTF-8 \
                            -Dsonar.host.url=${SONARQUBE_URL}
                        """
                    }
                }
            }
        }

        stage("Wait for SonarQube Quality Gate") {
            when {
                expression { ENVIRONMENT != 'prod' }
            }
            steps {
                script {
                    def qualityGateStatus = 'PENDING'
                    def maxRetries = 3
                    def retryInterval = 30
                    def retries = 0

                    while (qualityGateStatus == 'PENDING' && retries < maxRetries) {
                        sleep(time: retryInterval, unit: 'SECONDS')
                        def qualityGate = waitForQualityGate()
                        qualityGateStatus = qualityGate.status

                        echo "Quality Gate status: ${qualityGateStatus}"
                        retries++
                    }

                    if (qualityGateStatus != 'OK') {
                        echo "Quality Gate failed: ${qualityGateStatus}"
                        currentBuild.result = 'FAILURE'
                        error("Pipeline aborted due to failing SonarQube Quality Gate.")
                    } else {
                        echo "SonarQube Quality Gates Passed"
                    }
                }
            }
        }

        stage('Pull Source Code and Setup') {
            agent { label "cca-${ENVIRONMENT}" }
            environment {
                CODE_PATH = "${env.WORKSPACE}"
            }
            steps {
                script {
                    echo "Starting Pull Source Code and Setup Stage"
                    dir("${CODE_PATH}") {
                        echo "Multibranch checkout already done — cleaning workspace"
                        sh """
                            git fetch --all
                            git reset --hard
                            git clean -fd
                            git checkout ${env.BRANCH_NAME} || git checkout -b ${env.BRANCH_NAME} origin/${env.BRANCH_NAME}
                            git pull origin ${env.BRANCH_NAME}
                        """
                    }

                    echo 'Pulling Summit Repository...'
                    dir("${CODE_PATH}/advisory-services-summit") {
                        if (fileExists('.git')) {
                            sh '''
                                git reset --hard
                                git clean -fd
                                if git branch -a | grep -q "remotes/origin/dev"; then
                                    git checkout dev
                                    git pull origin dev
                                else
                                    echo "Branch 'dev' does not exist. Falling back to 'main'..."
                                    git checkout main
                                    git pull origin main
                                fi
                                git lfs pull
                            '''
                        } else {
                            sh '''
                                rm -rf *
                                git clone git@github.com:AMD-DEAE-CEME/advisory-services-summit.git .
                                git checkout dev || git checkout main
                                git lfs pull
                            '''
                        }
                    }

                    echo 'Extracting package and copying contents...'
                    dir("${CODE_PATH}/advisory-services-summit") {
                        sh '''
                            if [ -f "package.tar" ]; then
                                tar -xvf package.tar
                                cd package/
                                cp -r * ../../
                            else
                                echo "Error: package.tar not found."
                                exit 1
                            fi
                        '''
                    }
                }
            }
        }

        stage('Setup Python Environment') {
            agent { label "cca-${ENVIRONMENT}" }
            steps {
                script {
                    echo "Starting Setup Python Environment Stage"
                    sh 'rm -rf venv'
                    sh '''
                        python3 -m venv venv
                        . venv/bin/activate
                        pip install -r requirements.txt
                    '''
                }
            }
        }

       stage('Application Deployment') {
    agent { label "cca-${ENVIRONMENT}" }
    steps {
        script {
            echo "Starting Application Deployment Stage"
            sh """
                echo 'Restarting the API service...'
                sudo systemctl daemon-reload
                sudo systemctl restart cca_api.service
                sleep 5
                echo 'Checking service status...'
                sudo systemctl is-active --quiet cca_api.service
            """
            echo "Service is running successfully!"
        }
    }
}
stage('Cleanup Jenkins Remoting Cache') {
  agent { label "cca-${ENVIRONMENT}" }
    steps {
        script {
            echo "Cleaning contents of remoting jarCache..."
            sh '''
            CACHE_DIR="$HOME/remoting/jarCache"
            if [ -d "$CACHE_DIR" ]; then
                rm -rf "$CACHE_DIR"/*
            else
                echo "Cache directory not found: $CACHE_DIR"
            fi
            '''
        }
    }
}
    }

    post {
        always {
            echo 'Pipeline completed.'
        }
        success {
            emailext(
                subject: "Build Successful: ${env.JOB_NAME} (CCA ${ENVIRONMENT.toUpperCase()}) #${env.BUILD_NUMBER}",
                body: """<p>Hi Team,</p>
                         <p>The build <b>${env.JOB_NAME}</b> with build number <b>#${env.BUILD_NUMBER}</b> in the <b>CCA ${ENVIRONMENT.toUpperCase()}</b> environment was successful.</p>
                         <ul>
                          <li><b>Job Name:</b> ${env.JOB_NAME}</li>
                          <li><b>Build Number:</b> #${env.BUILD_NUMBER}</li>
                          <li><b>Environment:</b> CCA ${ENVIRONMENT.toUpperCase()}</li>
                          <li><b>Build URL:</b> <a href="${env.BUILD_URL}">${env.BUILD_URL}</a></li>
                         </ul>
                         <p>Regards,
Jenkins</p>""",
                mimeType: 'text/html',
                to: 'dl.epycadvisorAutomationAlerts@amd.com'
            )
        }
        failure {
            emailext(
                subject: "Build Failed: ${env.JOB_NAME} (CCA ${ENVIRONMENT.toUpperCase()}) #${env.BUILD_NUMBER}",
                body: """<p>Hi Team,</p>
                         <p>The build <b>${env.JOB_NAME}</b> with build number <b>#${env.BUILD_NUMBER}</b> in the <b>CCA ${ENVIRONMENT.toUpperCase()}</b> environment has failed.</p>
                         <ul>
                          <li><b>Job Name:</b> ${env.JOB_NAME}</li>
                          <li><b>Build Number:</b> #${env.BUILD_NUMBER}</li>
                          <li><b>Environment:</b> CCA ${ENVIRONMENT.toUpperCase()}</li>
                          <li><b>Build URL:</b> <a href="${env.BUILD_URL}">${env.BUILD_URL}</a></li>
                         </ul>
                         <p>Regards,
Jenkins</p>""",
                mimeType: 'text/html',
                to: 'dl.epycadvisorAutomationAlerts@amd.com'
            )
        }
    }
}
 