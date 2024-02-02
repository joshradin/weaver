pipeline {
    agent { docker { image "rust:latest" } }
    stages {
        stage("build") {
            steps {
                sh "cargo build --workspace"
            }   
        }
        stage("check") {
            steps {
                 sh "cargo check --workspace"
                 sh "cargo test --workspace"
            }
        }
    }
}
