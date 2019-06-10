$( document ).ready(function() {
  var ctx = $("#myChart");
  var monthly_scores = document.monthly_scores;
  var catToneMap = {};
  var catGSMap = {};
  for (var month in monthly_scores) {
    for (var cat in monthly_scores[month]) {
      if (!(cat in catToneMap)) {
        catToneMap[cat] = [];
        catGSMap[cat] = [];
      }
      catToneMap[cat].push(monthly_scores[month][cat].tone);
      catGSMap[cat].push(monthly_scores[month][cat].gs);
    }
  }
  var datasets = [];
  var borderColor = [
      'rgba(255, 99, 132)',
      'rgba(54, 162, 235)',
      'rgba(255, 206, 86)',
      'rgba(75, 192, 192)',
      'rgba(153, 102, 255)',
      'rgba(255, 159, 64)'
  ];
  var i = 0;
  for (var cat in catToneMap) {
    datasets.push({
        label: cat,
        data: catToneMap[cat],
        borderWidth: 1,
        borderColor: borderColor[i%6],
        backgroundColor: borderColor[i%6],
        fill: false,
        lineTension: 0.1,
    });
    i += 1;
  }
  var charts_data = {
      labels: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
      datasets: datasets,
  };
  var myChart = new Chart(ctx, {
    type: 'line',
    data: charts_data,
    options: {
      scaleLabel: {
        labelString: 'Monthly Average Tone for events',
      },
      scales: {
        yAxes: [{
            scaleLabel: {
                display: true,
                labelString: "Avg. Tone of Events",
                // fontFamily: "Montserrat",
                // fontColor: "black",
            },
            ticks: {
                beginAtZero:true
            }
        }]
      },
    }
  });
});
