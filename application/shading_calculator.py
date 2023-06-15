import numpy as np
import math
import pandas as pd


def generate_shading_arrays_for_pv_panel_group(pv_panel_group, shading_boxes, shading_cylinders):
    """
    Generates a shading array for a pv panel group by determining the fraction of the pv panel group that
    will be shaded by the boxes and cylinders at a set zenith and azimuth angles in 5 degree increments range from 0-90
    and 0-355 degrees respectively. The 3D co-ordinate system is defined such that the y-axis runs true
    south-north, with the north direction being positive, the x-axis runs west-east with the east direction being
    positive, and the z-axis runs down-up, with the up direction being positive. All distance values are metres.

    Examples:
        >>> panel_group = [(0, 0, 0), (2, 0, 0), (2, -1.415, 1.415), (0, -1.415, 1.415)]
        >>> boxes = [{'points': [(0, 0), (2, 0), (2, 2), (0, 2)], 'height': 2}]
        >>> cylinders = []
        >>> shading_array = generate_shading_arrays_for_pv_panel_group(panel_group, boxes, cylinders)

        >>> shading_array['s0'][:10]
        [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

        >>> len(shading_array['s0'])
        72

        >>> len(shading_array.keys())
        91

        >>> list(shading_array.keys())[:10]
        ['s0', 's1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9']


    Args:
        pv_panel_group: list[tuple] definition of a panel group is a list of four tuples where each tuple is the x,y,z
            co-ordinates of a corner of the panel group in 3D space. The order in which the corners are provided should
            be such that adjacent corners are consecutive in the list, and the first and last corners are adjacent.
            Note that this function assumes each panel group is a rectangle or square. An example set of pv
            panel groups would be [[(0, 0, 0), (6, 0, 0), (6, -1.4, 1.4), (0, -1.4, 1.4)]], in this case there is one
            group which is 6 metres long, running west-east, approx 2-wide, at a tilt of 45 degrees facing north.
        shading_boxes: list[dict] Each dictionary in the list defines a 3D box that could shade a panel
            group. Each box is a rectangular prism defined by 4 points that describe the corners of the prism
            (tuples with x,y values), and one height value, the points must be in order such that adjacent corners are
            consecutive in list, and the first and last corners are adjacent. An example box dictionary is
            {'points': [(0,0), (0,1), (2,1), (2,0)], 'height': 3]}.
        shading_cylinders: list[dict] Each dictionary in the list defines a 3D Cylinder that could shade a
            panel group. Each cylinder is described by a centre point, a radius, and a height value. An example
            cylinder dictionary is {'centre': (0,0), 'radius': 1, 'height': 3]}.

    Returns: dict{list} The keys in the dictionary are
        's0', 's1' . . . 's90' where the number after 's' is the zenith angle for the values in the corresponding list.
        Each list in the dictionary has 72 values, which correspond to 72 azimuth angles in 5 degree increments, i.e.
        azimuth angles from 0 to 355 degrees. Each value in the list is a float between zero and one that specifies the
        fraction of the panel group that will be shaded if the sun was at the corresponding angle. An example list for
        two panel groups would look like
        [{'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]},
         {'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]}]
    """
    points = generate_grid_of_points_on_panel_group(pv_panel_group, max_grid_space=0.30, buffer_from_edge=0.30,
                                                    precision=3)
    box_sides = generate_box_sides(shading_boxes)
    shading_arrays = generate_shading_arrays_for_points(points, box_sides, shading_cylinders)
    shading_array = aggregate_shading_arrays(shading_arrays)
    shading_array = format_shading_array(shading_array)
    return shading_array


def generate_grid_of_points_on_panel_group(panel_group, max_grid_space, buffer_from_edge, precision=3):
    """
    Generate a set of points in 3D space in a grid on the panel group, the grid spacing is determined such that the
    minimum number of points are used while keeping the spacing below max_grid_space. The grid starts within
    the panel group at the distance buffer_from_edge.

    Examples:

    This is a simple example where the panel group lies on the x-y plane. The panel group size and the buffer mean
    the grid points will be space 1 m apart, starting at 1 running to 9 on the x-axis, and starting at 1 and running
    to 19 on the y-axis.

    >>> rectangle_coords = [(0, 0, 0), (10, 0, 0), (10, 20, 0), (0, 20, 0)]
    >>> max_space = 1.0
    >>> buffer = 1.0
    >>> points = generate_grid_of_points_on_panel_group(rectangle_coords, max_space, buffer)
    >>> points[0]
    (1.0, 1.0, 0.0)

    >>> points[-1]
    (9.0, 19.0, 0.0)

    Args:
        panel_group: list[tuple] A list of four tuples where the tuple is the x,y,z co-ordinates of a corner of the
            panel group in 3D space. The order in which the corners are provided should be such that adjacent corners
            are consecutive in the list, and the first and last corners are adjacent. Note that this function assumes
            each panel group is a rectangle or square. An example pv panel group would be
            [(0, 0, 0), (6, 0, 0), (6, -1.4, 1.4), (0, -1.4, 1.4)], in this case the group is 6 metres long, running
            west-east, approx 2-wide, at a tilt of 45 degrees facing north.
        max_grid_space: float the maximum allowed space between points on the grid.
        buffer_from_edge: float the distance in from the edge to start the grid.
        precision: int the number of decimal places in the co-ordinates of the grid points
    """

    assert len(panel_group) == 4, "There must be 4 coordinates to define a rectangle."

    def interpolate(a, b, t):
        return a * (1 - t) + b * t

    def distance(a, b):
        return np.sqrt(np.sum((b - a) ** 2))

    p0, p1, p2, p3 = np.array(panel_group)

    # Calculate buffer factors based on distance and buffer
    buffer_factor_edge1 = buffer_from_edge / distance(p0, p1)
    buffer_factor_edge2 = buffer_from_edge / distance(p0, p3)

    # Calculate inner rectangle coordinates
    inner_p0 = interpolate(p0, p1, buffer_factor_edge1) + interpolate(p0, p3, buffer_factor_edge2) - p0
    inner_p1 = interpolate(p1, p0, buffer_factor_edge1) + interpolate(p1, p2, buffer_factor_edge2) - p1
    inner_p2 = interpolate(p2, p3, buffer_factor_edge1) + interpolate(p2, p1, buffer_factor_edge2) - p2
    inner_p3 = interpolate(p3, p2, buffer_factor_edge1) + interpolate(p3, p0, buffer_factor_edge2) - p3

    # Calculate grid size based on max_space
    edge1_length = distance(inner_p0, inner_p1)
    edge2_length = distance(inner_p0, inner_p3)

    n = max(2, int(np.ceil(edge1_length / max_grid_space)) + 1)
    m = max(2, int(np.ceil(edge2_length / max_grid_space)) + 1)

    points = []

    for i in range(n):
        for j in range(m):
            t1 = i / (n - 1)
            t2 = j / (m - 1)

            # Find two points on opposite sides of the inner rectangle that are in line with the final grid point
            # we are calculating the position of.
            temp1 = interpolate(inner_p0, inner_p1, t1)
            temp2 = interpolate(inner_p3, inner_p2, t1)

            # Interpolate between the two points we just found to find the final grid point.
            points.append(tuple(interpolate(temp1, temp2, t2).round(precision)))

    return points


def generate_box_sides(shading_boxes):
    """
    Reformat the shading box data so each side is defined separately.

    Args:
        shading_boxes: list[dict] Each dictionary in the list defines a 3D box that could shade a panel
            group. Each box is a rectangular prism defined by 4 points that describe the corners of the prism
            (tuples with x,y values), and one height value, the points must be in order such that adjacent corners are
            consecutive in list, and the first and last corners are adjacent. An example box dictionary is
            {'points': [(0,0), (0,1), (2,1), (2,0)], 'height': 3]}.

    Returns:
        list[dict] list of side definitions. Each side is defined using two x,y points, the height of
        the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
        {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}
    """
    shading_box_sides = []
    for box in shading_boxes:
        shading_box_sides.append(compose_box_side_definition(box, 0, 1))
        shading_box_sides.append(compose_box_side_definition(box, 1, 2))
        shading_box_sides.append(compose_box_side_definition(box, 2, 3))
        shading_box_sides.append(compose_box_side_definition(box, 3, 0))
    return shading_box_sides


def compose_box_side_definition(box, point_1, point_2):
    side_points = [box['points'][point_1], box['points'][point_2]]
    side_vector_normal = calculate_vector_normal(
        box['points'][point_1] + (0,),
        box['points'][point_2] + (0,),
        box['points'][point_1] + (box['height'],)
    )
    return {'points':side_points, 'height': box['height'], 'vector_normal': side_vector_normal}


def calculate_vector_normal(p1, p2, p3):
    """
    Compute the normal vector of a plane defined by three points

    Args:
        p1: tuple
        p2: tuple
        p3: tuple

    Returns:
        numpy array that represents the normal vector
    """
    # Convert points to numpy arrays
    p1, p2, p3 = np.array(p1), np.array(p2), np.array(p3)

    # Compute two vectors that lie on the plane
    v1 = p2 - p1
    v2 = p3 - p1

    # Compute the cross product of v1 and v2 to get the normal vector
    normal = np.cross(v1, v2)

    # Normalize the normal vector
    normal = normal / np.linalg.norm(normal)

    return tuple(normal)


def generate_shading_arrays_for_points(points, shading_sides_boxes, shading_cylinders):
    """
    For a list of points in 3D space generate an array of 0s and 1s for each point to specify if the point would be
    shaded by the 3D objects if the sun was at the given angles. A 0 indicates the point would not be shaded and 1
    indicates it would be shaded.

    Examples:

    Args:
        points: list[tuple(int)] a list of  x, y, z co-ordinates of the points. All values are in metres.
        shading_sides_boxes: list[dict] list of side definitions. Each side is defined using two x,y points, the height
            of the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}
        shading_cylinders: list[dict] a list of dictionaries. Each dictionary defines a 3D Cylinder that could shade the
            point. Each is cylinder is described by a centre point, a radius, and a height value. An example cylinder
            dictionary is {'centre': (0,0), 'radius': 1, 'height': 3]}. All values are in metres.
        angles: list[tuple] a set of altitude and azimuth angles from the point to check if the sun were at this angle
            would the point be shaded. First value in the tuple is altitude, second is azimuth.

    Returns: list[dict{list}] Each dictionary in the list is a shading array. The keys in the dictionary are
        's0', 's1' . . . 's90' where the number after 's' is the zenith angle for the values in the corresponding list.
        Each list in the dictionary has 72 values, which correspond to 72 azimuth angles in 5 degree increments, i.e.
        azimuth angles from 0 to 355 degrees. Each value in the list is a float between zero and one that specifies the
        fraction of the panel group that will be shaded if the sun was at the corresponding angle. An example list for
        two panel groups would look like
        [{'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]},
         {'s0': [0.0, 0.0, ... 0.0],  's1': [0.0, 0.0, ... 0.0], ... 's90': [0.0, 0.0, ... 0.0]}]
    """
    angle_vectors = generate_vectors_of_angles_in_shading_array_format()
    shading_arrays = []
    for point in points:
        shading_arrays.append(generate_shading_array_for_point(point, shading_sides_boxes, shading_cylinders,
                                                               angle_vectors.copy()))
    return shading_arrays


def generate_vectors_of_angles_in_shading_array_format():
    """
    Create a dictionary in the shading array format where the values are tuples specify the direction of the angle
    as an x, y, z vector.

    Example:

    >>> generate_vectors_of_angles_in_shading_array_format()

    Returns: dict{list[tuple]} The keys in the dictionary are
        's0', 's1' . . . 's90' where the number after 's' is the zenith angle for the values in the corresponding list.
        Each list in the dictionary has 72 values, which correspond to 72 azimuth angles in 5 degree increments, i.e.
        azimuth angles from 0 to 355 degrees. Each value in the list is a tuple that specifies the x, y, z components of
        a line at the corresponding zenith and azimuth angle. An example dictionary would look like
        {'s0': [(0.0, 1.0, 1.0), (0.08715574274765817, 0.9961946980917455, 1.0), ...],
         's1': [(0.0, 1.0, 0.9998476951563913), (0.08715574274765817, 0.9961946980917455, 0.9998476951563913), ...],
         ...
         's90': [(0.0, 1.0, 6.123233995736766e-17), (0.08715574274765817, 0.9961946980917455, 6.123233995736766e-17), ...]}

    """

    data_rows = []
    for zenith in range(0, 91, 1):
        for azimuth in range(0, 360, 5):
            x = math.sin(math.radians(azimuth))
            y = math.cos(math.radians(azimuth))
            z = math.cos(math.radians(zenith))
            data_rows.append((azimuth, zenith, (x, y, z)))

    angles_and_vectors = pd.DataFrame(data_rows, columns=['azimuth', 'zenith', 'vector'])

    return angles_and_vectors


def aggregate_shading_arrays(shading_arrays):
    shading_arrays = pd.concat(shading_arrays)
    shading_arrays = shading_arrays.loc[:, ['azimuth', 'zenith', 'shaded']]
    shading_arrays['shaded'] = np.where(shading_arrays['shaded'], 1, 0)
    shading_array = shading_arrays.groupby(['azimuth', 'zenith'], as_index=False)['shaded'].sum()
    shading_array['shaded'] = np.where(shading_array['shaded'] >= 1, 1, 0)
    return shading_array


def format_shading_array(shading_array):
    re_formated_array = {}
    zenith_groups = shading_array.groupby(['zenith'], as_index=False)
    for group, data in zenith_groups:
        data = data.sort_values('azimuth')
        re_formated_array['s' + str(group)] = list(data['shaded'])
    return re_formated_array


def generate_shading_array_for_point(point, shading_boxes_sides, shading_cylinders, angles):
    """
    For a given point in 3D space generates an array of 0s and 1s to specify if the point would be shaded by the
    3D objects if the sun was at the given angles. A 0 indicates the point would not be shaded and 1 indicates it
    would be shaded.

    Examples:

    Args:
        point: tuple(int) the x, y, z co-ordinates of the point. All values are in metres.
        shading_boxes_sides: list[dict] list of side definitions. Each side is defined using two x,y points, the height
            of the box, and the pre-computed vector normal of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}
        shading_cylinders: list[dict] a list of dictionaries. Each dictionary defines a 3D Cylinder that could shade the
            point. Each is cylinder is described by a centre point, a radius, and a height value. An example cylinder
            dictionary is {'centre': (0,0), 'radius': 1, 'height': 3]}. All values are in metres.
        angles: list[tuple] a set of altitude and azimuth angles from the point to check if the sun were at this angle
            would the point be shaded. First value in the tuple is altitude, second is azimuth.

    Returns: list[int] of length angles, a 0 value indicates the point would not be shaded, and 1 value indicates the
        point would be shaded.
    """
    angles['shaded'] = angles.apply(
        lambda x: check_if_angle_shaded(point, x['vector'], shading_boxes_sides, shading_cylinders), axis=1)
    return angles


def check_if_angle_shaded(point, vector, shading_boxes_sides, shading_cylinders):
    line = {'point': point, 'vector': vector}
    for side in shading_boxes_sides:
        if check_if_line_goes_through_box_side(line, side):
            return True
    for cylinder in shading_cylinders:
        if check_if_line_intercepts_cylinder(line, cylinder):
            return True
    else:
        return False


def check_if_line_goes_through_box_side(line, side):
    """
    Check if a line goes through the side of box. By first checking if the line goes through the plane that the side
    of the box sits on. If its does, finding the point that the line intersects the plane. Lastly checking if that
    point is within the bounds of that defined the edges of the side of the box.

    Examples:

    Args:
        line: dict{tuple} A line defined using a point and vector. The dictionary would be
            {'point': (x, y, z), 'vector':(a, b, c)}. Where x, y, z are the co-ordinates of a point that the line
            passes through. and a, b, and c are the components of the vector in the x, y, z direction. So an example
            line that passes through the origin and heads directly north at altitude of 45 degrees would be
            {'point': (0, 0, 0), 'vector':(0, 1, 1)}
        side: dict{} definition of side using two x,y points, the height of the box, and the pre-computed vector normal
            of the plane the side sits on. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3, 'vector_normal': (0, 1, 0)]}

    Returns: Boolean
    """
    plane = {'point': side['points'][0] + (side['height'],), 'vector_normal': side['vector_normal']}
    intercept = find_line_intercept_with_plane(line, plane)
    if intercept == 'parallel':
        return False
    elif intercept == 'coincident':
        return True
    else:
        return check_if_intercept_point_within_bounds_of_side(intercept, side)


def find_line_intercept_with_plane(line, plane):
    """
    Finds where a line in 3D space intercepts a plane in 3D space.

    If the line is coincident with the plane the string 'coincident' is returned, if the line is parallel to the plane ]
    the string 'parallel' is returned, if line intercepts the plane at a point the (x, y, z) value is returned.

    Maths behind the function:

    Given a line defined by a point (x0, y0, z0) and a vector <a, b, c> we can describe the line in parametric form
    using the equations:

        x = x0 + a*t
        y = y0 + b*t
        z = z0 + c*t

    Given the vector normal of plane <A, B, C> and a point on the plane (x1, y1, z1) we can describe the plane using
    the equation

        A(x - x1) + B(y - y1) + C(z - z1) = 0

    To check if the line we can substitute the equations for the point into the equation for the line and solve for t.

        A(x0 + a*t - x1) + B(y0 + b*t - y1) + C(z0 + c*t - z1) = 0

        t = (A*(x1 - x0) + B*(y1 - y0) + C*(z1 - z0)) / (A*a + B*b + C*c)

    If the denominator (A*a + B*b + C*c) the line is either parallel to the plane or coincident with the plane. If the
    point defining the line is also on the plane then the line is coincident with plane, otherwise the line is
    parallel to the plane.

    If the denominator does not equal zero, then we can find a value for t, and then the x, y, z co-orindates
    of the point of interception using the parametric form of the line.

    Examples:

    Define a line that one metre down the y-axis and goes straight up, and a plane that runs along the x and z axis.
    Then attempt to compute the intercept, the results should be 'parallel'.

    >>> line0 = {'point': (0, 1, 0), 'vector': (0, 0, 1)}

    >>> plane0 = {'point': (0, 0, 0), 'vector_normal': (0, 1, 0)}

    >>> find_line_intercept_with_plane(line0, plane0)
    'parallel'

    Now change the line to go through the origin, the result should be 'coincident'.

    >>> line0 = {'point': (0, 0, 0), 'vector': (0, 0, 1)}

    >>> find_line_intercept_with_plane(line0, plane0)
    'coincident'

    Now change the line back to the first starting point, but slow it back towards the plane at 45 degrees. The
    result should be an intercept 1 metre above the origin.

    >>> line0 = {'point': (0, 1, 0), 'vector': (0, -1, 1)}

    >>> find_line_intercept_with_plane(line0, plane0)
    (0.0, 0.0, 1.0)

    Args:
        line: dict{tuple} A line defined using a point and vector. The dictionary would be
            {'point': (x, y, z), 'vector':(a, b, c)}. Where x, y, z are the co-ordinates of a point that the line
            passes through. and a, b, and c are the components of the vector in the x, y, z direction. So an example
            line that passes through the origin and heads directly north at altitude of 45 degrees would be
            {'point': (0, 0, 0), 'vector':(0, 1, 1)}
        plane: dict{} definition of plane using an x,y,z point and the vector normal of the plane. An example dictionary
            would be {'point': (0, 0, 3), 'vector_normal': (0, 1, 0)]}

    Returns: str or tuple e.g. 'coincident', 'parallel', or (x, y, z)
    """

    # Unpack inputs into variable values used in documentation
    # Point on line
    x0 = line['point'][0]
    y0 = line['point'][1]
    z0 = line['point'][2]
    # Vector of line
    a = line['vector'][0]
    b = line['vector'][1]
    c = line['vector'][2]
    # Point on plane
    x1 = plane['point'][0]
    y1 = plane['point'][1]
    z1 = plane['point'][2]
    # Vector normal of plane
    A = plane['vector_normal'][0]
    B = plane['vector_normal'][1]
    C = plane['vector_normal'][2]

    solution_numerator = (A*(x1 - x0) + B*(y1 - y0) + C*(z1 - z0))
    solution_denominator = (A*a + B*b + C*c)

    # Check if line is coincident or parallel to plane.
    if solution_denominator == 0:
        # Check if line is coincident to plane.
        if A*(x0 - x1) + B*(y0 - y1) + C*(z0 - z1) == 0:
            return 'coincident'
        else:
            return 'parallel'

    t = solution_numerator / solution_denominator

    x = x0 + a * t
    y = y0 + b * t
    z = z0 + c * t

    return x, y, z


def check_if_intercept_point_within_bounds_of_side(point, side):
    """
    Check if a point in 3D space is within the bounds of the side of a box. Assumes the side of the box is vertical.

    Maths behind function:

    Because the sides of the box are vertical we can check if the x,y co-ordinates of the intercept are within the
    x,y limits of the side separately to consider the height of the intercept.

    If the two points defining the side of the box are (x0, y0) and (x1, y1), and the points defining the intercept are
    (xI, yI), then intercept is within the bounds of the box if x1 is between x0 and x1, and yI is between y0 and y1.
    Which, without know if x0 > x1 or not, we can check by testing is xI is closer to x0 and x1 than x0 is to x1, and
    the same for y.

    Then we just additionally check if the z of the intercept is less than the height of the side.

    Examples:

    Example where point is within side.

    >>> side_1 = {'points': [(0, 10), (10, 0)], 'height': 3}
    >>> point_1 = (5, 5, 2)
    >>> check_if_intercept_point_within_bounds_of_side(point_1, side_1)
    True

    Example where point is too high

    >>> point_2 = (5, 5, 3.1)
    >>> check_if_intercept_point_within_bounds_of_side(point_2, side_1)
    False

    Example where point is outside the side because it's too far in the x direction

    >>> point_3 = (11, -1, 2)
    >>> check_if_intercept_point_within_bounds_of_side(point_3, side_1)
    False

    Example where point is outside the side because it's not far enough in the x direction

    >>> point_4 = (-1, 11, 2)
    >>> check_if_intercept_point_within_bounds_of_side(point_4, side_1)
    False

    Args:
        point: tuple the x, y, co-ordinates of the point.
        side: dict{} definition of side using two x,y points, and the height of the box. An example dictionary would be
            {'points': [(0,0), (0,1)], 'height': 3]}
    """
    xI = point[0]
    yI = point[1]
    zI = point[2]
    x0 = side['points'][0][0]
    y0 = side['points'][0][1]
    x1 = side['points'][1][0]
    y1 = side['points'][1][1]

    # x-axis distance between points.
    xI2x0 = abs(xI - x0)
    xI2x1 = abs(xI - x1)
    x02x1 = abs(x0 - x1)

    if xI2x0 <= x02x1 and xI2x1 <= x02x1:
        x_value_within_side = True
    else:
        x_value_within_side = False

    # y-axis distance between points.
    yI2y0 = abs(yI - y0)
    yI2y1 = abs(yI - y1)
    y02y1 = abs(y0 - y1)

    if yI2y0 <= y02y1 and yI2y1 <= y02y1:
        y_value_within_side = True
    else:
        y_value_within_side = False

    if side['height'] >= zI >= 0:
        z_value_within_side = True
    else:
        z_value_within_side = False

    return x_value_within_side and y_value_within_side and z_value_within_side


def check_if_line_intercepts_cylinder(line, cylinder):
    # define variables for convenience
    x0, y0, z0 = line['points']
    a, b, c = line['vector']
    h, k = cylinder['centre']

    # Coefficients for the quadratic equation At^2 + Bt + C = 0
    A = a**2 + b**2
    B = 2*a*(x0 - h) + 2*b*(y0 - k)
    C = (x0 - h)**2 + (y0 - k)**2 - cylinder['radius']**2

    # Compute the discriminant
    discriminant = B**2 - 4*A*C

    if discriminant < 0:
        # If discriminant is less than 0, the line does not intersect the cylinder.
        return False
    else:
        # Otherwise, solve for t
        t1 = (-B - np.sqrt(discriminant)) / (2*A)
        t2 = (-B + np.sqrt(discriminant)) / (2*A)

        # Check the z-coordinates of the intersection points
        z1 = z0 + t1 * c
        z2 = z0 + t2 * c

        z_min = 0
        z_max = cylinder['height']

        if (z_min <= z1 <= z_max) or (z_min <= z2 <= z_max):
            # If either z1 or z2 is within the cylinder height limits, return True
            return True

    # If none of the conditions are met, return False
    return False



