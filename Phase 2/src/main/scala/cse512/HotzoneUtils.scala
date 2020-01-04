package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART return true 
    val pt = pointString.split(",")
    val pt_x = pt(0).trim().toDouble
    val pt_y = pt(1).trim().toDouble
    
    val rect = queryRectangle.split(",")
    val rect_x_1 = rect(0).trim().toDouble
    val rect_y_1 = rect(1).trim().toDouble
    val rect_x_2 = rect(2).trim().toDouble
    val rect_y_2 = rect(3).trim().toDouble
    
    var min_x = List(rect_x_1, rect_x_2).min
    var max_x = List(rect_x_1, rect_x_2).max
    
    var min_y = List(rect_y_1, rect_y_2).min
    var max_y = List(rect_y_1, rect_y_2).max
  
    
    if(pt_x >= min_x && pt_x <= max_x && pt_y >= min_y && pt_y <= max_y) {
      return true
    } else {
      return false
    }
    
  }

  // YOU NEED TO CHANGE THIS PART

}
