import json
import time
import os
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

suite = [
    {'numberOfThreads': 1},
    {'numberOfThreads': 2},
    {'numberOfThreads': 4},
    {'numberOfThreads': 6},
    {'numberOfThreads': 8},
]

x = []
y_avg = []
y_min = []
y_p98 = []
y_ops = []

out_dir = 'testsuite/runs/' + str(int(time.time()))

os.makedirs(out_dir)

for test in suite:
    result_file_name = '{}/test_{}.json'.format(out_dir, test['numberOfThreads'])
    cmd = tpl.format(numberOfItems=1000,
                     numberOfThreads=test['numberOfThreads'],
                     resultFileName=result_file_name)
    subprocess.call(cmd, shell=True)

    with open(result_file_name) as result_file:
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

plt.savefig(out_dir + '/' + 'test_result.png', dpi=150)