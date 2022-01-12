

from include.Tracking import Tracking


track=Tracking('test')
track2=Tracking('failure test')
track.record_parameter('a',42.)
track2.record_parameter('a',42)
track2.record_parameter('l',['a','b','c'])
track2.failed("This is a test of failure.")
track.succeeded()
print(track.get_summary())
print("----------")
print(track2.get_summary())
