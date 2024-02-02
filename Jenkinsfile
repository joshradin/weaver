pipeline {
    agent { docker { "rust:latest" } }
    stages {
        stage("check") {
            sh "cargo check --workspace"
        }
        stage("test") {

        }
    }
}
