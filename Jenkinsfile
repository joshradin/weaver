pipeline {
    agent { 
        kubernetes {
            yaml '''
                apiVersion: v1
                kind: Pod
                spec:
                    containers:
                        - name: rust
                          image: rust:latest
                          tty: true
            '''
        } 
    }
    stages {
        stage("build") {
            steps {
                container("rust") {
                    sh "cargo build --workspace"
                }
            }   
        }
        stage("check") {
            steps {
                container("rust") {
                    sh "cargo check --workspace"
                    sh "cargo test --workspace"
                }
            }
        }
    }
}
