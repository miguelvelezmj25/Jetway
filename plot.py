import matplotlib.pyplot as pyplot


def plot_graph(x, y, x_label, y_label, axis):
    print 'Plotting graph'

    pyplot.plot(x, y, 'bs', x, y, 'b-')
    pyplot.xlabel(x_label)
    pyplot.ylabel(y_label)
    pyplot.axis(axis)

    pyplot.show()

    print 'Done plotting'


if __name__ == '__main__':
    x = [1, 3, 5, 8, 10, 20, 35, 50, 70, 100]
    y = [4.097354323, 5.165355576, 5.965716878,
         6.841929825, 7.549285714, 10.06184211,
         13.97782738, 16.50467105, 17.9311585,
         18.43324561]
    axis = [x[0], x[-1], 0, 25]
    x_label = 'Number of Refinements'
    y_label = 'Average CPU Usage'

    plot_graph(x, y, x_label, y_label, axis)
