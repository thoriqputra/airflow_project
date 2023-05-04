# class

class Person:
    # Variables
    id = 0

    # Constructor
    def __init__(self,name=None,age=0):
        self.name = name
        self.age = age
        self.id = id

    # Deconstructor
    def __del__(self):
        print("Object Delete")

    def __str__(self):
        return "name : "+self.name

    # Methods
    def myfunc(self):
        print("hell my nameis " + self.name)

    def hello(self):
        print("hello")

    def sayHello(self, name):
        print("Hello "+name)

    def printAll(self):
        print("id = ", self.id)
        print("name = ", self.name)
        print("age = ", self.age)
# How to Run
p1 = Person("John", 36)
p1.hello()
p1.myfunc()
p1.sayHello("mr.abc")
print(p1)
p1.printAll()

p2 = Person()
print(p2)
