import signal
import string
import subprocess
from dataclasses import dataclass
from datetime import datetime
import os
import time
import psutil
import argparse
from settings import *


def sleep_until(t):
    sleep_duration = t-time.time()
    if(sleep_duration > 0):
        time.sleep(sleep_duration)

def mem2int(mem: str):
    suffix= mem[-1]
    if not suffix.isalpha():
        return int(mem)
    else:
        return int(float(mem[:-1])*memsuffix2dec[suffix])


def get_available_file(f_name: str):
    format_args = [t[1] for t in string.Formatter().parse(f_name) if t[1] is not None]
    if len(format_args) == 0:
        return f_name # Can override file, trust the user
    elif len(format_args) == 1:
        f_num = 0
        while os.path.exists(f_name.format(f_num)):
            f_num += 1
        return f_name.format(f_num)
    else:
        raise ValueError("Output file have more than one formattable (i.e. {}) field.")

class LogRow:
    mem: int
    virt: int
    cpu: float
    d_time: datetime
    valid: bool

    def __init__(self, pid):
        data = os.popen(f'top -b -n1 -p{pid}').read().splitlines()[-1].split()
        try:
            self.mem = mem2int(data[field2col['MEM']])
            self.virt = mem2int(data[field2col['VIRT']])
            self.cpu = float(data[field2col['CPU']])
            self.d_time = datetime.now()
            self.valid = True
        except ValueError as err:  # Process is killed, unparseable row
            self.mem = self.virt = self.cpu = self.d_time = None
            self.valid = False

    @staticmethod
    def get_header():
        return ', '.join(['Time', 'RES', 'VIRT', 'CPU'])

    def __str__(self):
        if self.valid:
            return ', '.join([str(x) for x in [self.d_time, self.mem, self.virt, self.cpu]])
        else:
            return ''

class MemLogger:
    pid: int
    t_int: float
    duration: float
    flush_rate: int
    output_file: str
    verbose: bool
    logs_list: list
    log_pos: int

    def __init__(self, pid, t_int, duration, flush_rate, output_file, verbose):
        self.pid = pid
        self.t_int = t_int
        self.duration = duration
        self.flush_rate = flush_rate
        self.output_file = output_file
        self.verbose = verbose
        self.logs_list = [None]*flush_rate
        self.log_pos = 0

    def start_logging(self):
        if self.duration > 0:
            target_time = time.time() + self.duration
        else:
            target_time = time.time() + 1e10  # More than 300 years, infinity more or less
        next_log_time = time.time()
        self.log_pos = 0
        proc = psutil.Process(self.pid)
        with open(self.output_file, 'w') as f:
            f.write(LogRow.get_header() + '\n')
            while (proc.is_running() and time.time() < target_time):
                if(proc.status() == psutil.STATUS_ZOMBIE):
                    proc.kill()
                    break
                self.logs_list[self.log_pos] = LogRow(self.pid)
                if self.logs_list[self.log_pos].valid == False:
                    break
                self.log_pos += 1
                if self.log_pos >= self.flush_rate:
                    self.flush_logs(f)
                next_log_time += self.t_int
                sleep_until(next_log_time)
            self.flush_logs(f)

    def flush_logs(self, f):
        for i in range(self.log_pos):
            if self.verbose:
                print(self.logs_list[i])
            f.write(str(self.logs_list[i]) + '\n')
        self.log_pos = 0


def parse_it():
    print("TODO")

def main():
    parser = argparse.ArgumentParser(description="Log the memory and cpu consumption of a process.")
    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument('-l', '--log', action='store_const', dest='action', const='log', default='log', help='Run the logger.')
    action_group.add_argument('-d', '--display', action='store_const', dest='action', const='parse', help='Run the parser.')
    # logging arguments
    parser.add_argument('command', type=str, nargs='*', default=None, help='A command to run and log.')
    parser.add_argument( '-p', '--pid', type=int, help='A process id to log.')
    parser.add_argument('-r', '--rate', type=float, default=DEFAULT_SAMPLE_RATE,
                        help=f'Sampling rate in seconds. Default={DEFAULT_SAMPLE_RATE}.')
    parser.add_argument('-t', '--duration', type=float, help='Time duration to log.')
    parser.add_argument('-f', '--flush-rate', type=int, default=DEFAULT_FLUSH_RATE,
                        help=f'How many samples to hold until writing to file. Default={DEFAULT_FLUSH_RATE}')
    parser.add_argument('-o', '--output-file', type=str, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument('-v', '--verbose', action='store_true', help='Write log to stdout.')

    args = parser.parse_args()
    if args.action == 'log':
        if args.pid is None and not args.command:
            parser.error("Missing process id to log (--pid/-p <pid>) or command to run and log (<command>).")
        if args.pid is not None and args.command:
            parser.error("Got PID and a command, only one is allowed.")
        if args.pid:
            pid = args.pid
            self_spawned_proc = None
        else:
            self_spawned_proc = subprocess.Popen(args.command)
            pid = self_spawned_proc.pid

        if args.rate <= 0:
            parser.error("Sampling rate must be positive")
        t_int = args.rate

        if args.duration is None:
            duration = NO_DURATION_CONST
        elif args.duration <= 0:
            print(f"Negative logging duration was supplied. "
                  f"Logging will continue until process {pid} is killed or finished.")
            duration = NO_DURATION_CONST
        else:
            duration = args.duration

        if args.flush_rate <= 0:
            parser.error("Flushing rate must be positive")
        flush_rate = args.flush_rate
        output_file = get_available_file(args.output_file)

        if args.verbose:
            verbose = True
        else:
            verbose = False

        print(pid)
        print(t_int)
        print(duration)
        print(flush_rate)
        print(output_file)
        print("log")
        logger = MemLogger(pid, t_int, duration, flush_rate, output_file, verbose)
        logger.start_logging()
        if self_spawned_proc:
            print("Logging stopped, waiting for logged process to terminate.")
            self_spawned_proc.wait()


    if args.action == 'parse':
        print("parse, todo")

if __name__ == '__main__':
    main()