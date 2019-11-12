import pandas as pd
import re


incorrects_file = 'incorrect.txt'


def main():
    fields = ('pxd', 'run', 'path', 'file', 'expected', 'actual')
    incorrect = pd.DataFrame(columns=fields)
    pxd_re = re.compile('(PXD\d+)')
    with open(incorrects_file, 'r') as in_file:
        for line in in_file.readlines():
            fields2 = ('run', 'file', 'expected', 'actual')
            line = line.rstrip()
            values = re.split('Incorrect |Expected |Actual ', line)
            f = values[1].split('/', 2)
            r = values[1].rsplit('/', 1)
            matches = pxd_re.search(r[0])
            if matches:
                pxd = matches.group(1)
            else:
                pxd = ''
            row = {'pxd': pxd, 'run': f[1], 'path': values[1], 'file': r[1], 'expected': values[2],
                   'actual': values[3]}
            incorrect = incorrect.append(row, ignore_index=True)
    incorrect.to_excel(r'incorrect.xlsx', index=False)
    print()


if __name__ == "__main__":
    main()

exit(0)
