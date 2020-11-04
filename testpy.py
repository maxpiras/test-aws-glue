path_to_data = "s3://zus-qa-s3/algoritmo1"
with open('path_to_data + /test.txt', 'w') as file:
  file.write('prova write')
  print('SUCCESS')
