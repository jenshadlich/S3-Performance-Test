import json
import subprocess
import matplotlib.pyplot as plt

#'--signerOverride S3Signer '
tpl = 'java -jar target/s3pt.jar ' \
      '--bucketName testbucket ' \
      '--endpointUrl localhost:8888 ' \
      '--operation=UPLOAD ' \
      '--size 4k ' \
      '--http ' \
      '--usePathStyleAccess ' \
      '-n {numberOfItems} ' \
      '-t {numberOfThreads} ' \
      '--resultFileName {resultFileName}'

#tpl = 'java -jar target/s3pt.jar ' \
#      '--bucketName testbucket ' \
#      '--endpointUrl localhost:8888 ' \
#      '--operation=RANDOM_READ ' \
#      '--keyFileName s3keys.txt ' \
#      '--http ' \
#      '--usePathStyleAccess ' \
#      '-n {numberOfItems} ' \
#      '-t {numberOfThreads} ' \
#      '--resultFileName {resultFileName}'

tests = [
    {'numberOfThreads': 1, 'resultFileName': 'test_1.json'},
    {'numberOfThreads': 2, 'resultFileName': 'test_2.json'},
    {'numberOfThreads': 4, 'resultFileName': 'test_4.json'},
    {'numberOfThreads': 6, 'resultFileName': 'test_6.json'},
    {'numberOfThreads': 8, 'resultFileName': 'test_8.json'},
]

x = []
y_avg = []
y_min = []
y_p98 = []
y_ops = []

for test in tests:
    cmd = tpl.format(numberOfItems=100,
                     numberOfThreads=test['numberOfThreads'],
                     resultFileName=test['resultFileName'])
    subprocess.call(cmd, shell=True)

    with open(test['resultFileName']) as result_file:
        data = json.load(result_file)
        x += [test['numberOfThreads']]
        y_avg += [data['avg']]
        y_min += [data['min']]
        y_p98 += [data['p98']]
        y_ops += [data['ops']]

plt.figure(1, figsize=(6, 8), dpi=100)
plt.subplot(211)
plt.plot(x, y_avg, label='avg')
plt.plot(x, y_min, label='min')
plt.plot(x, y_p98, label='p98')

plt.title('response times', fontweight='bold')
# plt.xlabel('#threads')
plt.ylabel('ms')
plt.legend()

plt.subplot(212)
plt.title('throughput', fontweight='bold')
plt.plot(x, y_ops, label='ops')
plt.legend()
plt.xlabel('#threads')

plt.subplots_adjust(hspace=.3)

plt.savefig('test_result.png', dpi=150)