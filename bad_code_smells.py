
# This file intentionally contains MANY code smells for SonarQube validation
# There are excessive comments, duplicated code, long methods, poorly named variables, etc.

# -----------------------------------------
# DUPLICATED CODE
# -----------------------------------------
def calculate_sum(a, b):
    # This function sums two numbers
    total = a + b
    return total

def calculate_total(a, b):
    # This function ALSO sums two numbers (duplicated logic)
    tot = a + b
    return tot

# -----------------------------------------
# LONG METHOD WITH BAD STRUCTURE
# -----------------------------------------
def long_method():
    # This method does too many things and has bad naming
    x = 10
    y = 20
    z = x + y
    print("Sum:", z)
    # Unnecessary comments
    # This prints something else
    for i in range(5):
        print("Loop", i)
    # More unrelated logic
    data = [1,2,3,4]
    for d in data:
        print("Data item:", d)
    # More unrelated logic
    if z > 20:
        print("Greater than 20")
    else:
        print("Less or equal to 20")

# -----------------------------------------
# BAD CLASS DESIGN
# -----------------------------------------
class BadClass:
    # Class with many responsibilities and bad naming
    def __init__(self):
        self.x = 10
        self.y = 20
        self.data = []

    def doEverything(self):  # violates SRP
        # Too much logic here
        print("Values:", self.x, self.y)
        self.data.append(self.x)
        self.data.append(self.y)
        print("Data list:", self.data)
        # Unnecessary duplication
        self.data.append(self.x)
        self.data.append(self.y)
        print("Data list again:", self.data)

    def badNaming(self, a, b):  # vague name
        return a * b

# -----------------------------------------
# DEAD CODE
# -----------------------------------------
def unused_function():
    # This function is never used
    return "I am dead code"

# -----------------------------------------
# MAIN EXECUTION BLOCK
# -----------------------------------------
def main():
    print(calculate_sum(5, 10))
    print(calculate_total(5, 10))
    long_method()
    bc = BadClass()
    bc.doEverything()
    print(bc.badNaming(3, 4))

if __name__ == '__main__':
    main()
