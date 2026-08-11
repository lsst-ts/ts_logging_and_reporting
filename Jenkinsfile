pipeline {
  options {
    disableConcurrentBuilds()
  }

  agent any

  environment {
    dockerImageName = "rubincr.lsst.org/nightlydigest-backend:"
    dockerImageTag = "alpha"
    dockerImage = ""
    pythonVersion = "3.13"
  }

  stages {
    // stage("Setup and run pre-commit") {
    //   agent {
    //     docker {
    //       alwaysPull true
    //       image 'lsstts/develop-env:develop'
    //       args "--entrypoint=''"
    //     }
    //   }
    //   steps {
    //     script {
    //       sh """
    //         source /home/saluser/.setup_dev.sh || echo loading env failed. Continuing...
    //         generate_pre_commit_conf --skip-pre-commit-install
    //         pre-commit run --all
    //       """
    //     }
    //   }
    // }

    stage("Run tests") {
      agent {
        docker {
          alwaysPull true
          image 'lsstts/develop-env:develop'
          args "--entrypoint=''"
        }
      }
      steps {
        script {
          sh """
            source /home/saluser/.setup_dev.sh || echo loading env failed. Continuing...
            pip install -e .
            pytest -v
          """
        }
      }
    }

    stage("Build Docker image") {
      when {
        anyOf {
          branch "develop"
          branch "alpha-release"
        }
      }
      steps {
        script {
          dockerImageName = dockerImageName + dockerImageTag
          echo "dockerImageName: ${dockerImageName}"
          dockerImage = docker.build(dockerImageName, "--build-arg py_version=${pythonVersion} -f docker/Dockerfile-deploy .")
        }
      }
    }
    
    stage("Push Docker image") {
      when {
        anyOf {
          branch "develop"
          branch "alpha-release"
        }
      }
      steps {
        script {
          docker.withRegistry("https://rubincr.lsst.org/", "nexus3-lsst_jenkins") {
            dockerImage.push()
          }
        }
      }
    }
  }
}
