import asyncio
import base64
import tempfile
import unittest
from pathlib import Path

from rededup.utils.processor import Processor


class ProcessorTest(unittest.TestCase):
    def test_simple_hashing(self):
        completed = False

        with tempfile.NamedTemporaryFile() as f:
            data = base64.b64decode(
                "iXwelN3g/GwjCy4bASc58zn1PEGk7CBtlKx2IC6rbCZnh8W0FbBxFu2VxAsOg"
                "5h11XETs6/uKDaYw+9paMRU5rTSh3dLnHj3113uLvH1ewc7U0GorofxMzPzmf"
                "O9C1kG9A/Fq81lb40DQJxyIGGPr+CBMuUE/e7rO9UtgNlbjZOraR1hEnMmHmo"
                "S5EvE8ckhZPbsYBA8ON7f1ZSlnAn1cDDCYdaE465msfpOqEK4gA4WLtcSCr9Z"
                "zTqDwzpTBV/yqswY5OyenqKWOks0QK1zCQ7g8krwdtwh6l2fpBGGgTNK7FhPd"
                "+5pIQ6sxnY5J4OLLxkgTlWL8feoq1uVoCkaoNmSLjswsHvSMiqwTTWsF06eh9"
                "jiiskGukwOWtqdOTbtZ8EZE/OQiWJ1hZAvNXUve2ImaRExIQurW4qr9OJN73n"
                "9rgFPe920pw9kd/OdkzNqUiTYZSw4Omo7uM75HkKtsZOpgmuMg2QiVLQ4cbhG"
                "FwsSEbPKIOIWQFpTxpqOWW4RGnLv3qyVoT5qjJw/Bt6Ln7sOSCHlsEQ24jALg"
                "VW0C5J6XHiwhMN3T/BziXJIggVmOLaZTLKbnBEkZRmKY3ayYEKwitWULyQsT5"
                "nDFgKVOlDZJh8BkncZFGm5qULUJrxDydvVYHJd/rDy0bnU+wpshMi/yKsp1jS"
                "cFCbWry2qVgB3K1iGpoTthnf3E5dMFHavu4UIcATXsXKKLdmKaDweBTN/MQPd"
                "k4/NYmXU0j0HF9xEIuHDPqTF21ExszOzp7njEZp6w/egahUJfCKOZW7SigiC+"
                "jlOPlpJebRqWvr9lPY2ky28XH4C4bKuuoyGQsS+GV3JdfD+I8Psff+uKFQbcc"
                "2rVZsi7vxqVWdWDLgiQYYZtNwQ14cH5ciw2zu2PkzOA3MQ14QyL8mtfTNjgoU"
                "zmCQgTOTHL/iyYexQJ/zR01/idkW2ypTXnj68SA++IkwcgJBPyAg55uZeZxNn"
                "V4Irc2agpcw40ye0s4P0jdOKPllyRdw5QFmDpipldWGzr1ghLlGYjhXq2E9tT"
                "OqjmtyrLZ9nrhW+PFSCmpaOBqNQF62E/V3tLr7wdAwNZrYAddhHdL+sv/2H4S"
                "5nhDkoe5rklI3nG+zkygIrq/nlpdRKd9+Uw/nHpSwMNN+EcXmft7YpmpKL4oe"
                "W5Hiyv9T9Icft/LxNdrb1QobMEW8kduzcXpOE8OkjV87NJnTmCcSHP+0qsYvU"
                "o43vAotLHPeMeDTDyqJXFZzkJtq9iyYuXknmjI7dqrsZFm/9zi5/qZ2TQz6lu"
                "f90xeY3/dHGAZUKQDWgNs6bqc7/3TnwGg31YSflliLSAW7Nqo3CN9GmC9dY2z"
                "q0bloiBt2PMdslSeNo5OvLs9GEk94rwWbbd2fs2ecepM37jp9DzLkfWaU/KK8"
                "B7KxByaRKol0usZml5NS9khjn3NsIvrJd6yYgNj48KuljV5RfVfHn0CMtNjyR"
                "MJ0ehLQx3xy8/TfZupQ7PQYuoW1BfC1xcJMSG7+w1Qp0K8p/YiF8/vPFdZ9pG"
                "mRFMH9oAGDhRlOwsvSx/sJMylSLIeSPwXNKAZlOkMm1YeN3waicDjMaXvyZ3i"
                "elkqCt3TUZlplhaQiXTrU/6Oxx/YpbApdsaonaMSXKJr+u44+MoR8dRsTBIns"
                "2v1PAJMczmE2wtxzpfpNFiCKSu0V0FxLSw0n4e8QBWJi4XRwjvxevOD+vzkRZ"
                "plmqFKCxA8vdhjuYvjRxU2RhAiEO07oaaJ0Uw1ZqKJlvA3DyAVHJELGYUmGiV"
                "We5qOL/xcVaivbdyj/FiMn/zTovwr/QMiuCETalz99E1KyFfxxGvjCuz0XNvW"
                "lAkXtcwjim0vYC5w9V/Ki0fc+5rU+gdbACi0KkZDuaHGq8+5HncjAqsy72iME"
                "CjC3qjhr4J/FazpH+Dlz+Z1re0tuee5r7Z3c1pGOoiLlzzTk/GObH39QctFl/"
                "U57zOMvTg3yQtNPdcnWAPVNqa2Slreqlz5Dqwv/CLSj4fOa74U6mjMbTH9WYK"
                "54okR7JNtqTObfWQJdGe37CAyc1jgre8tKTk9ujc/0GSHhvQ+HoZIpAByqzoO"
                "qMmhRTWeCCR3nlmZFgJVY+EPMiSdUT+i8UDTuHH78euxB1oIsHqRJU58RrvwL"
                "24GYlq83BIaVhQmvvfU2rZzfeHA0TQExYyPUCsmTeI15f9NuTDBy9FqJYBtkv"
                "X6H9Q+RC7rGqQiAXt6dJ4zCIrMew8ev6WRV6NKpiiyPvDksntWgUie5uthOF+"
                "sFYxy63u2tEvHW3sUEnS6dctTbCrG3bQyAgnZrGMk0WXbCAOX79DSwZox9oWV"
                "pMbD3kS4+kC6XA2vAVMnvHXNYM6yPV1asdrQGVC+GZ8sL6wt0fj0xj2z3sdZ/"
                "tj5Q0tuzfkjr3FCmqv684n3pjUCEXwSMBBK99vXaHLOG+CHTXmKSvLMKTRppZ"
                "Ot/pxrtx01m8jihj/6N5/S738KG78RMRKeGu5el3zbkxHsANkTWskxR8qhd7M"
                "Y2UiHwXKAk2RHk/mtgWogHdky5kAiLVudlNv4Nfv5r3NZurE3Ds7egRK6Y6i/"
                "veluf51SQ1wfg5wJVdEhHjw+AyddvK335Ok6IPZIrlEkiKmEwKk5zJ2Wt6z9z"
                "tq+fyFMTnGR4a+hChRmEOHZ5aNBlies1sdaUFSAvANNgOkGsCLeOBZdzxx0JM"
                "M1G1tL7VG+riAn/3yEJPBQOLkDFmEWlTebr5k/BfYtPSrONP35NUKQveqcnqE"
                "HEA82OEguv2gLb6SE9E9q2VnH2gFJXprU5f7tTM4rc7SUnIzKWIk5GmWsrBmo"
                "WLY+qKpIoWh+hDhjXxaKfjY8Ty2mrTNNUI+7QTrID/zCskSuDx8z/AvfpUGi7"
                "ssFrlX2f37R7uvOTwXw57zxh/Wdt8uHXBSUKL6ODUntX/1Y+sskR2v7VYSrls"
                "d1rwdgAarcc4ADQ0fQXM+MrnnK2EoOk1wwcpWiRu3ubhpSWyPwil+GGIcztMQ"
                "RBfPjqFmdkL6OcnWTz2sz0lUXSK3j/hD1gskqxQ3LLMK639JS9sNP+GguTDj1"
                "qnpMDvVUhES0KhNkr7EUUEQNkY5kOWmoYSaN1PibReNkTGcdKYNAEjJCUQ79U"
                "yzztoSKHpN8BwCVuIbp5JhL7R51xA6sH/+ccNmEGAmU8MnHWy/H0pDN9MCxRD"
                "HURrgDQ5Izx7sG7Ozc1OcuvvcPdgWd/JSLPbfXuO9xIuU9r2g8QV4Zb3o8ZxJ"
                "DUpBvpoBSed6UIZpvvMUtx1huKs3mYPwcAfGeELOzouMfzNzwPaqgHxgla/s0"
                "yQEmRKOIayKND7HNdQfvHwwt8n7Mej6PLQgIoDNfjqP6BR4wsEw80LNdtGyyj"
                "lC0XkkU/Z6wOVOhZ6ZLzaX4GY/uMzFdDorlAcKmimxQeRj5a3yhKfEH4vocgF"
                "lwfh7mYQZ5i/OCuMOjZ9Q2y/ZQZ4xIbowoLyg9xZd6fbOeWfdj76mfok+tixA"
                "V1ryEZh2QXycFi6vHOzVfc1L89aWhEzVcsNjNN+y3uGu3/ABNY91pVEIyFs1T"
                "Izszi1TPkYCiibIwT5laRJKhoj3BYxJOrWwZHz3jj0svE4W0hd9IpYAIK/QMF"
                "oDL2pVg6CMjQG9J3GxhSYHUvDm16OqhI1eWQxH+JxwVFPKHeI3n1dV+TnVktR"
                "0t9Qk/URhpUNwpuWI7uJrJW0POngpzwcaEVBN9XdoGNnrC9dpudc+1vygMKRW"
                "Iinhk9ExmSN6JCq2rCuaaNdKXlrs+w/LTq3GKs3IJcvM3KeCWxgrzmboyCn82"
                "xxKOEODYxYSzhpTdemQVOAySNZcX31aX0zc6hO6T8QIX7daFKZ1FtmwaJwNsb"
                "9/O/h38ynzkC1JZ0vPplTyMVGy68kUCn0C1l82WKEJZYOHftCsAR2xoNzOvRU"
                "HB9UtxHhuZDGPRlwOH90J1J9hbVFIvlnuMAYrHK0fnaHXWjXg6CZHVM0fhNlR"
                "ZFIba2+NxLRa0AB4jOIRA26kAXz938Cxnr0NqdBfDutnyhEQoJlS20+58XBl+"
                "33AdZ0N0syeKeVrwHaN4+d/SiD6SSkBX0OqaTg31x82y8QP3K5Fdhp99S2uuF"
                "g/THGmIa8u7NkATvl72YlcRaTPfH6Z9KS/of3JsZlDMEHas6smDu8ROSVfARn"
                "MUaMhj1hgGiPcdokokEZWq2PSeDACfJanNsL0aMf0YFBEYbEcbbPgHdg69E3B"
                "Mjiotbplt/uJ9zyTAWgRTkO3lVJ+wcXm3fFkZtxR8qvAaTTmvFBSmmjO+BPwt"
                "LyN1UeVQK43ZhdzGyxAn3aTgbNurStj9OnxbbNpO/2vGdpZLttWWbHxP3Z2+l"
                "Rrw7gcaQMdj55jFvUnbve3/5HUypW1jMZLMoYvVcRjgzEOKLF4S8Ar7PgRoIF"
                "UWpSkEU3DIV7xXZDfpGwkFJBr+ShD/E+PPZ0++47GA2hGin69rIg658N3HyrW"
                "eXbUQPEQCwPo7d83Vbla2nL7GOYmaeBeYu3wMQC99qbfewxQkAUPW19r+M1px"
                "n+aib4XEUVQJdsepeV3Y/vHsf+aFfrVvIdnUUct/CRVNKdlsaw74a+yeW2Bka"
                "tW3+rg8tQODdaKtzK3FFF+UiraAuYwoBDUazCIKzjn4u+eUUuizt3n9tEbgp8"
                "eRxxCqS7zxCK7KsluOo6fbwsMWPeNJW62SGTBAbQuktRBjVZijaGNBajdYXpc"
                "iwh+cLK3tP8qE0HMIZgaOm44/PWscx6QI/M1Iw3/4YQ2HvBWvak7KK2tLyW41"
                "k2hvhxAcPYe+OsyJJplB1ZTpbfhpditCbfXWwXzyhL+Bf71vG/qR//+iLIuRM"
                "p/QbUwMhAz7UmFYbGNtwNWTaR+E44WFp76rgzyafnNW9qYtHZUPCFa85/ESb/"
                "PaIETri4eKtdyi2ELX9H1T2kLzGjy0V+zn+QDg5SVGYjkApc7HXaQ0Zdy7pEc"
                "rW4ECxTSK+IbybwiyrikRbFwc4b1uajmzyz2YnlUgVMZy/CrfxDleNU6Hedrv"
                "urSJCVqPsZu2FffwoBjh0DiFO/UzIZUI+CXIYGCbPvdqm/NZttiiIS+rqEoSk"
                "IAcCrpaQ0eop1VCEZfqwjyXIiACDzhTQ/14sG1IsRPcINpsy9ZZOmSxfid6LG"
                "2tPL9sti+oZkWXjpwt7pJvV4kkbTZN4aM3pwOmV7SXlj02keq+9OENN1uFeDe"
                "1TqY6VM9pTXNxCgqc5dWxtYeOV8L19NXeobatZ2ZMSBHvX9jxo3FJpjlCIGBd"
                "kXQdUKzxTROmilyGbVnoxpPvjQzcdKrfTaUAkzuFW9oCl1Br9D6JxI5ksSKZ0"
                "x8qSyxxv2+Pl/ZY0nvgNAlXjh4kJgbAsQGzOHBqm0EQh89HM9HHSuVwPvvxw7"
                "7rntnoaYZSEVG0dPSKuSKXoEmh7TZ1e/MSWasZl9f56+cLnEXahh2MdxpCcuI"
                "NuxvyGI2ySYlKm9wfSd9HbtKEDa54LmPw==")
            f.write(data)
            f.flush()

            with Processor() as processor:
                async def verify():
                    digest = await processor.sha256(Path(f.name))

                    nonlocal completed
                    self.assertEqual(
                        digest.hex(), "d8539a89a9bd029c9b9815c54f43dbd9c7c4bd10508190d162de6a2e23f28ceb")
                    completed = True

                asyncio.run(verify())

        self.assertTrue(completed)


if __name__ == '__main__':
    unittest.main()
