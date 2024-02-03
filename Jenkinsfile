pipeline {
    triggers {
        pollSCM "H/5 * * * *"
    }
    agent { 
        kubernetes {
            inheritFrom 'default'
            yaml '''
            spec:
                containers:
                    - name: rust
                      image: rust:alpine
                      tty: true
            '''
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
                    sh "cargo install nextest --locked"
                    sh "cargo-nextest run --workspace --profile ci --hide-progress-bar --no-capture"
                }
                junit testResults: "target/nextest/ci/junit.xml"
            }
        }
    }
}
