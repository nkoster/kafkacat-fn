'use strict'

module.exports = async (event, context) => {

  const {topic, partition, offset} = event.body

  const broker = 'localhost'

  console.log(topic, partition, offset)

  const { exec } = require('child_process')
  const execute = (cmd, callback) => exec(cmd, (_, stdout) => callback(stdout))
  
  const data = await new Promise((resolve, reject) => {
    try {
      const cmd = `kafkacat -C -b ${broker} -t ${topic} -p ${partition} -o ${offset} -c 1 -e -q`
      execute(cmd, result => resolve(result))
    } catch (err) {
      console.log(err.message)
      reject({})
    }
  }) 

  return context
    .headers({ 'Content-type': 'application/json' })
    .status(200)
    .succeed(data)
    
}
