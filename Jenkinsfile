pipeline {
    triggers {
        pollSCM "H/5 * * * *"
    }
    agent { 
        node {
            label "rust && alpine"
        }
    }
    stages {
        stage("Install requirements") {
            steps {
                container("rust") {
                    sh 'curl -LsSf https://get.nexte.st/latest/linux-arm -o nextest.tar.gz'
                    sh 'tar -xvf nextest.tar.gz -C ${CARGO_HOME:-~/.cargo}/bin'
                }
            }
        }
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
                    sh "cargo-nextest run --workspace --profile ci --hide-progress-bar --no-capture"
                }
                junit testResults: "target/nextest/ci/junit.xml"
            }
        }
    }
}
