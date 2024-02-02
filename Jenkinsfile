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
                    sh "cargo install cargo-nextest"
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
                    junit testResults: "target/nextest/ci/junit.xml"
                }
            }
        }
    }
}
