counts = [0 for i in range(100) ]

lineNo = 0

with open("output", 'r') as f:
	lines = f.readlines()

for line in lines:
	lineNo += 1
	words = line.split(" ")
	if words[0] == "+":
		counts[int(words[1])] += 1
		if int(words[1]) == 14:
			print "+14 on " + str(lineNo)
	elif words[0] == "-":
		counts[int(words[1])] -= 1
		if int(words[1]) == 14:
			print "-14 on " + str(lineNo)

for i in range(len(counts)):
	if counts[i] != 0:
		print str(i) + ": " + str(counts[i])
		
