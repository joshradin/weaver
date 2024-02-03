pipeline {
    triggers {
        pollSCM "H/5 * * * *"
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        disableConcurrentBuilds()
    }
    agent { 
        kubernetes {
            inheritFrom 'rust'
        }
    }
    stages {
        stage("Check") {
            steps {
                container("rust") {
                    sh "cargo check --workspace"
                }
            }   
        }
        stage("Tests") {
            steps {
                container("rust") {
                    sh "cargo install cargo-nextest --locked"
                    sh "cargo nextest run --workspace --profile ci"
                }
            }
        }
    }
    post {
        always {
            junit testResults: "target/nextest/ci/junit.xml"
        }
    }
}
