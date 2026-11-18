// Jenkins pipeline with the same stages as the GitHub Actions workflow.
// Assumes a Jenkins agent with Docker installed. Everything runs inside
// containers, so the agent doesn't need Python or Java.
// Not yet run on a real Jenkins server - see README "What's not done yet".

pipeline {
    agent any

    environment {
        IMAGE   = "game-data-pipeline:${env.BUILD_NUMBER}"
        NETWORK = "gdp-ci-${env.BUILD_NUMBER}"
        PG      = "gdp-pg-${env.BUILD_NUMBER}"
    }

    options {
        timeout(time: 30, unit: 'MINUTES')
        timestamps()
    }

    stages {
        stage('Build image') {
            steps {
                // Dev dependencies included so we can lint and test in the same image.
                sh 'docker build --build-arg INSTALL_DEV=true -t $IMAGE .'
            }
        }

        stage('Lint') {
            steps {
                sh 'docker run --rm $IMAGE sh -c "ruff check . && ruff format --check ."'
            }
        }

        stage('Test') {
            steps {
                sh '''
                    docker network create $NETWORK
                    docker run -d --name $PG --network $NETWORK \
                        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=games_test postgres:16
                    for i in $(seq 1 30); do
                        docker exec $PG pg_isready -U test -d games_test && break
                        sleep 2
                    done
                    docker run --rm --network $NETWORK \
                        -e TEST_DATABASE_URL=postgresql+psycopg://test:test@$PG:5432/games_test \
                        $IMAGE pytest -v
                '''
            }
        }

        stage('Push image') {
            when { branch 'main' }
            steps {
                // Next step: push to Amazon ECR so ECS/EventBridge can run it.
                // Needs an `aws-ecr` credential in Jenkins and the ECR_REPO variable set.
                echo 'TODO: docker tag $IMAGE $ECR_REPO:latest && docker push $ECR_REPO:latest'
            }
        }
    }

    post {
        always {
            sh 'docker rm -f $PG || true; docker network rm $NETWORK || true'
        }
    }
}
