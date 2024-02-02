pipeline {
    triggers {
        pollSCM "H/5 * * * *"
    }
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
                          imagePullPolicy: IfNotPresent
            '''
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
