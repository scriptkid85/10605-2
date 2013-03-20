TESTFILE="test.txt"

all: MyTrainertrain.class MyCountAdder.class MyTester.class

test: MyTrainertrain.class MyCountAdder.class MyTester.class abstract.tiny.train
	cat abstract.tiny.train | java -Xmx128m MyTrainertrain | sort -k1,1 | java -Xmx128m MyCountAdder | java MyTester -t $(TESTFILE)

MyTrainertrain.class: MyTrainertrain.java
	javac MyTrainertrain.java

MyCountAdder.class: MyCountAdder.java
	javac MyCountAdder.java

MyTester.class: MyTester.java
	javac MyTester.java

demo: MyTrainertrain.class MyCountAdder.class MyTester.class abstract.tiny.test abstract.tiny.train
	cat abstract.tiny.train | java MyTrainertrain | sort -k1,1 | java MyCountAdder | java MyTester -t abstract.tiny.test

clean:
	rm -rf *class
