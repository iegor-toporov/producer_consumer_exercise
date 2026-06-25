# Producer-Consumer exercise
A very simple exercise written in C that has 3 threads: a producer, a consumer, ad adjuster.
The role of the adjuster is to check every 2 seconds if the production rate is too fast for the fixed consumer rate, and increase or decrease the production rate accordingly by a fixed value, based on a simple threshold.