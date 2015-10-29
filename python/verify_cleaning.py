#!/usr/bin/env python3
import os

result_dir = 'result/'
input_dir = 'input/'


results = os.listdir(result_dir)

for result_name in results:
    input_name = result_name.replace('subm', 'test').replace('_nist7.txt', '.csv')
    print('reading ' + result_name + ' and ' + input_name + '...')
    line = 0
    reasons = {'nearby' : 0, 'same' : 0, 'Inconsistent' : 0, 'negative' : 0}
    with open(result_dir + result_name, 'r') as result_file, open(input_dir + input_name, 'r') as input_file:
        inp_vals = list(input_file)[1:]
        res_vals = list(result_file)
	
        if len(inp_vals) != len(res_vals):
            print(result_name + 'different number of lines!')
            continue
	
        for res, inp in zip(res_vals, inp_vals):
            res_val = res.strip().split('\t')
            inp_val = inp.strip().split(',')
            line += 1
            if res_val[0] == '1' and res_val[1] != inp_val[3]:
                print(result_name + ' order wrong in line ' + str(line))
                print(res_val, inp_val)
                break

            if res_val[0] == '0':
                if abs(float(res_val[1]) - float(inp_val[3])) < 1e-6:
                    print(result_name + ' correction equal to origin flow')
                for key in reasons:
                    if key in res_val[2]:
                        reasons[key] += 1
                        break

    print(result_name)
    for key, value in reasons.items():
        print(key, value)
    print(str(line) + ' lines\n')


