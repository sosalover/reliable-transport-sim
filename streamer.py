# do not import anything else from loss_socket besides LossyUDP
import struct
from concurrent.futures import ThreadPoolExecutor

from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import time


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.src_port = src_port
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.data_buffer = {}
        self.ack_buffer = {}
        self.send_number = 0
        self.sequence_number = -1
        self.closed = False
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # for now I'm just sending the raw application-level data in one UDP payload
        length = len(data_bytes)
        i = 0
        done = False
        while not done:
            header = "ACK? 0\n" + "snum:" + str(self.send_number) + "\n"
            if i + 1000 >= length:
                end = length
                done = True
            else:
                end = i + 1000
            part1 = header.encode('utf8')
            part2 = data_bytes[i: end]
            part = part1 + part2
            self.socket.sendto(part, (self.dst_ip, self.dst_port))

            start_time = time.time()
            while self.send_number not in self.ack_buffer:
                time.sleep(0.1)
                if time.time() - start_time >= 0.25:
                    self.socket.sendto(part, (self.dst_ip, self.dst_port))
                    start_time = time.time()
            if not done:
                self.send_number += 1
            i = i + 1000
        self.send_number += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!

        while (self.sequence_number + 1) not in self.data_buffer:
            pass
        decoded_payload = self.data_buffer[self.sequence_number + 1]
        del self.data_buffer[self.sequence_number + 1]
        payload = decoded_payload.encode()
        self.sequence_number += 1

        return payload


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                data_string = data.decode('utf8')
                split = data_string.split("\n", 2)

                if len(split) <= 2:
                    continue
                if len(split[1][5:]) == 0:
                    continue
                ack_field = int(data.decode('utf8').split("\n", 2)[0][5])
                if not bool(ack_field):
                    data_sequence_number = int(split[1][5:])
                    self.data_buffer[data_sequence_number] = split[2]
                    header = "ACK? 1\n" + "snum:" + str(data_sequence_number) + "\n"
                    self.socket.sendto(header.encode("utf8"), addr)
                else:
                    data_sequence_number = int(split[1][5:])
                    self.ack_buffer[data_sequence_number] = True
            except Exception as e:
                print("listener died!")
                print(e)