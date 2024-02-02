pipeline {
    triggers {
        pollSCM "H/5 * * * *"
    }
    agent { 
        node {
            label "rust"
        }
    }
    stages {
        stage("Install requirements") {
            steps {
                container("rust") {
                    sh 'curl -LsSf https://get.nexte.st/latest/linux-arm | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin'
                }
            }
        }
        stage("Build") {
            steps {
                container("rust") {
                    sh "cargo check --workspace"
                    sh "cargo build --workspace"
                }
            }   
        }
        stage("Tests") {
            steps {
                container("rust") {
                    sh "cargo nextest run --workspace --profile ci"
                }
                junit testResults: "target/nextest/ci/junit.xml"
            }
        }
    }
}
