const AWS = require('aws-sdk');

const lambda = new AWS.Lambda({ region: 'eu-west-1' });

const tasks = [];

const vehicles = `
  AA08EXP,AA08EXP-0d9beefcac0ca0510090f5523bdc45b8
`.split(`\n`).filter(row => row !== '').map(row => {
  const [vrm, vehicleId] = row.replace(/\s/g, '').split(',');
  return {vrm, vehicleId}
})

console.log(vehicles);

const processVehicle = (index) => {
  const vehicle = vehicles[index];
  console.log('next', vehicle);
  const task = lambda.invoke({
    FunctionName: 'quote-engine-dispatcher-production',
    Payload: JSON.stringify({
      "type": "SESSION_START",
      "payload": {
        "sessionId": vehicle.vrm,
        "vrm": vehicle.vrm,
        "vehicleId": vehicle.vehicleId,
      }
    }),
    Qualifier: 'production',
  }).promise()
  .catch(err => console.log(err));

  tasks.push(task);

  if(index < vehicles.length - 1) {
    setTimeout(() => {
      processVehicle(index + 1)
    }, 1000);
  } else {
    console.log('end index %s', index);
  }
}

processVehicle(0);

return Promise.all(tasks).then(() => {
  console.log('done')
})