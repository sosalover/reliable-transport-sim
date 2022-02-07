# do not import anything else from loss_socket besides LossyUDP
import struct
from concurrent.futures import ThreadPoolExecutor

from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import hashlib
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
        self.fin_ack1_found = False
        self.fin_ack2_found = False
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

            header = "FIN? 0\n" + "ACK? 0\n" + "snum:" + str(self.send_number) + "\n"
            if i + 1000 >= length:
                end = length
                done = True
            else:
                end = i + 1000
            part1 = header.encode("latin1")
            part2 = data_bytes[i: end]
            part = part1 + part2
            hash_part = (str(hashlib.sha1(part).hexdigest()) + "\n").encode("latin1")
            part = hash_part + part
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
        payload = decoded_payload.encode("latin1")
        self.sequence_number += 1

        return payload


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        header = "FIN? 1\n" + "ACK? 0\n" + "snum:" + str(self.send_number) + "\n"
        fin_packet = header.encode("latin1")
        hash_part = (str(hashlib.sha1(fin_packet).hexdigest()) + "\n").encode("latin1")
        fin_packet = hash_part + fin_packet

        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        start_time = time.time()

        # search for ack for first fin
        while not self.fin_ack1_found:
            time.sleep(0.1)
            if time.time() - start_time >= 0.25:
                self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
                start_time = time.time()
        self.send_number += 1

        # search for ack for second fin
        fin2_packet = "FIN? 2\n" + "ACK? 0\n" + "snum:" + str(self.send_number) + "\n"
        fin2_packet = fin2_packet.encode("latin1")
        hash_part = (str(hashlib.sha1(fin2_packet).hexdigest()) + "\n").encode("latin1")
        fin2_packet = hash_part + fin2_packet
        while not self.fin_ack2_found:
            time.sleep(0.1)
            if time.time() - start_time >= 0.25:
                self.socket.sendto(fin2_packet, (self.dst_ip, self.dst_port))
                start_time = time.time()
        self.send_number += 1
        time.sleep(2)
        self.closed = True
        self.socket.stoprecv()

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                data_string = data.decode("latin1")
                split = data_string.split("\n", 4)

                if len(split) <= 2:
                    continue
                if len(split[2][5:]) == 0:
                    continue
                hash_field = split[0]
                hashed_packet = str(hashlib.sha1(data_string.split("\n", 1)[1].encode("latin1")).hexdigest())
                if hash_field != hashed_packet:
                    continue
                ack_field = int(data.decode("latin1").split("\n", 4)[2][5])
                fin_field = int(data.decode("latin1").split("\n", 4)[1][5])
                if fin_field == 1 and not bool(ack_field):
                    data_sequence_number = int(split[3][5:])
                    header = "FIN? 1\n" + "ACK? 1\n" + "snum:" + str(data_sequence_number) + "\n"
                    header = header.encode("latin1")
                    hash_part = (str(hashlib.sha1(header).hexdigest()) + "\n").encode("latin1")
                    header = hash_part + header
                    self.socket.sendto(header, addr)
                    second_fin = "FIN? 2\n" + "ACK? 0\n" + "snum:" + str(data_sequence_number + 1) + "\n"
                    second_fin = second_fin.encode("latin1")
                    hash_part = (str(hashlib.sha1(second_fin).hexdigest()) + "\n").encode("latin1")
                    second_fin = hash_part + second_fin
                    self.socket.sendto(second_fin, addr)
                elif fin_field == 2 and not bool(ack_field):
                    data_sequence_number = int(split[3][5:])
                    header = "FIN? 2\n" + "ACK? 1\n" + "snum:" + str(data_sequence_number + 1) + "\n"
                    header = header.encode("latin1")
                    hash_part = (str(hashlib.sha1(header).hexdigest()) + "\n").encode("latin1")
                    header = hash_part + header
                    self.socket.sendto(header, addr)
                elif not bool(ack_field):
                    data_sequence_number = int(split[3][5:])
                    self.data_buffer[data_sequence_number] = split[4]
                    header = "FIN? 0\n" + "ACK? 1\n" + "snum:" + str(data_sequence_number) + "\n"
                    header = header.encode("latin1")
                    hash_part = (str(hashlib.sha1(header).hexdigest()) + "\n").encode("latin1")
                    header = hash_part + header
                    self.socket.sendto(header, addr)
                else:
                    if fin_field == 1:
                        self.fin_ack1_found = True
                    elif fin_field == 2:
                        self.fin_ack2_found = True
                    else:
                        data_sequence_number = int(split[3][5:])
                        self.ack_buffer[data_sequence_number] = True
            except Exception as e:
                print("listener died!")
                print(e)