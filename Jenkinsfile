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
        stage("build") {
            steps {
                container("rust") {
                    sh "cargo check --workspace"
                    sh "cargo build --workspace"
                }
            }   
        }
        stage("check") {
            steps {
                container("rust") {
                    sh "cargo test --workspace"
                }
            }
        }
    }
}
