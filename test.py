# Initial File
inputString = input("Please input:")

a = inputString.index("(")
functionName = inputString[:a]
sql = inputString[a+1:len(inputString) - 1]

