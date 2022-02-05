# do not import anything else from loss_socket besides LossyUDP
import struct

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
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.buffer = {}
        self.send_number = 0
        self.sequence_number = -1

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # for now I'm just sending the raw application-level data in one UDP payload
        length = len(data_bytes)
        i = 0
        done = False
        while not done:
            header = "snum:" + str(self.send_number) + "\n"
            if i + 1000 >= length:
                end = length
                done = True

            else:
                end = i + 1000
            part1 = header.encode('utf8')
            part2 = data_bytes[i: end]
            part = part1 + part2
            self.socket.sendto(part, (self.dst_ip, self.dst_port))
            i = i + 1000
            if not done:
                self.send_number += 1
        self.send_number += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!

        while (self.sequence_number + 1) not in self.buffer:
            data, addr = self.socket.recvfrom()
            data_string = data.decode('utf8')
            split = data_string.split("\n", 1)
            data_sequence_number = int(split[0][5:])
            self.buffer[data_sequence_number] = split[1]
        decoded_payload = self.buffer[self.sequence_number + 1]
        del self.buffer[self.sequence_number + 1]
        payload = decoded_payload.encode()
        print(payload)
        self.sequence_number += 1
        # this sample code just calls the recvfrom method on the LossySocket

        # For now, I'll just pass the full UDP payload to the app

        return payload


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
