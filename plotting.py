import db_manager
import plot


def plot_data():
    db_manager.open_connection()

    statement = "SELECT configurationId, value, options " \
                "FROM measurements " \
                "JOIN configurations " \
                "ON id = configurationId " \
                "WHERE configurationId >= 55 " \
                "AND configurationId <= 65 " \
                "AND nfpId = 2"

    result = db_manager.execute(statement)

    value_number = 1
    row_counter = 0
    x_values = []
    y_values = []

    for row in result:
        value_counter = 0

        for value in row:
            if row_counter % 2 == value_number and value_counter == 1:
                y_values.append(float(str(value)))

            if row_counter % 2 == value_number and value_counter == 2:
                x_values.append(filter_options(value, 'num_refinements'))

            value_counter += 1

        row_counter += 1

    # print x_values
    # print y_values

    db_manager.close_connection()

    axis = [int(x_values[0]), int(x_values[-1]), 0, 25]
    x_label = 'Number of Refinements'
    y_label = 'Average CPU Usage'

    plot.plot_graph(x_values, y_values, x_label, y_label, axis)


def filter_options(options, criteria):
    option_value = {}

    words = str(options).split(',')
    words = map(str.strip, words)

    for word in words:
        hold = word.split(' ')
        option_value[hold[0]] = hold[1]

    return option_value[criteria]


if __name__ == '__main__':
    plot_data()
