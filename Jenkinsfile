pipeline {
    triggers {
        pollSCM "H/5 * * * *"
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        disableConcurrentBuilds()
    }
    agent { 
        kubernetes {
            inheritFrom 'rust'
        }
    }
    stages {
        stage("Verify Integrity") {
            steps {
                container("rust") {
                    sh "cargo check --workspace"
                    sh "cargo deny check"
                    sh "cargo audit"
                }
            }   
        }
        stage("Run Unit Tests") {
            steps {
                container("rust") {
                    sh "cargo nextest run --workspace --profile ci-unit -E 'kind(bin) + kind(lib)'"
                }
            }
        }
        stage("Run Integration Tests") {
            steps {
                container("rust") {
                    sh "cargo nextest run --workspace --profile ci-int -E 'kind(test)'"
                }
            }
        }
        stage("Build Artifacts") {
            steps {
                container("rust") {
                    sh "mkdir bins"
                    sh "cargo install --path ./crates/weaver-client --root ./bins"
                    sh "cargo install --path ./crates/weaver-daemon --root ./bins"
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: "bins/bin/*", fingerprint: true
            junit testResults: "target/nextest/ci/junit-*.xml"
        }
    }
}
