pipeline {
  agent any
  environment {
    COMMIT = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
  }
  stages {
    stage('Git pull') {
      steps {
        sh 'ssh crawler16 \'cd /home/omgili/files/DuplicateService && git checkout master && git pull\''
      }
    }
    stage('Run Playbook') {
      steps {
        sh 'ssh katie-002 \'source /home/omgili/.ansible/venv/bin/activate && cd /home/omgili/ansible/ && ansible-playbook playbooks/duplicate_service.yml --extra-vars=\'commit=$COMMIT\' -v\''
      }
    }
  }
}

