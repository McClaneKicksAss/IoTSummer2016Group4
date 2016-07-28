package iot.project;

import iot.project.CoordinateTransformExample.CoordinateTransformHelper;

public class GridHelper {
	
	// EPSG:3857 is a Spherical Mercator projection coordinate system popularized by web services such as Google and later
		// OpenStreetMap (http://wiki.openstreetmap.org/wiki/EPSG:3857)
		CoordinateTransformHelper transformation;
		private double dx;
		private double dy;
		private double gridSizeX;
		private double gridSizeY;
		private double[] max;
		private double[] min;
		
		
		/**
		 * helper class for transforming coordinates into xy values
		 * @param transformation
		 * @param minLonLat: for setting min values for latitude and longitude
		 * @param maxLonLat: for setting max values for latitude and longitude
		 * @param gridElements: set the number of cells per x and y
		 * @throws Exception
		 */
		public GridHelper(CoordinateTransformHelper transformation, double minLonLat[], double maxLonLat[], double gridElements)
				throws Exception {

			this.transformation = transformation;

			//transform min and max values
			this.max = transformation.transform(maxLonLat);
			this.min = transformation.transform(minLonLat);

			// adjusting the system to canada
			this.dx = max[0] - min[0];
			this.dy = max[1] - min[1];
			
			// calculate the size of the cells
			this.gridSizeX = dx / gridElements;
			this.gridSizeY = dy / gridElements;

		}

		
		/**
		 * method to calculate the x and y values from latitude and longitude
		 * @param lat: latitude value
		 * @param lon: longitude value
		 * @return integer array containing the transformed x and y values
		 * @throws Exception
		 */
		int[] toGrid(double lat, double lon) throws Exception {

			double[] xyCoords = transformation.transform(new double[] { lat, lon });
			//System.out.println("Transformed values: lat " + xyCoords[0] + " long " + xyCoords[1]);
			xyCoords[0] -= this.min[1];
			xyCoords[1] -= this.min[0];
			//System.out.println("Transformed values with min value: lat " + xyCoords[0] + " long " + xyCoords[1]);
			

			return new int[] { (int) ((xyCoords[1] / this.gridSizeX)), (int) ((xyCoords[0] / this.gridSizeY)) };
		}

}