pipeline {
    agent { docker { image "rust:latest" } }
    stages {
        stage("check") {
            sh "cargo check --workspace"
        }
        stage("test") {
            sh "cargo test --workspace"
        }
    }
}
