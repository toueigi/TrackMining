/*
 * Copyright 2005 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.common.geometry;


import com.google.common.annotations.GwtCompatible;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

/**
 * The Earth modeled as a sphere.
 *
 * <p>Provides many convenience functions so that it doesn't take 2 lines of code just to do a
 * single conversion. Note that the conversions between angles and distances on the Earth's surface
 * provided here rely on modeling Earth as a sphere; otherwise a given angle would correspond to a
 * range of distances depending on where the corresponding line segment was located.
 *
 * <p>More sophisticated Earth models (such as WSG84) should be used if required for accuracy or
 * interoperability.
 *
 * @author ericv@google.com (Eric Veach)
 * @author norris@google.com (Norris Boyd)
 */
@JsType
@GwtCompatible
public class S2Earth {
  private S2Earth() {}

  /**
   * Returns the Earth's mean radius, which is the radius of the equivalent sphere with the same
   * surface area. According to NASA, this value is 6371.01 +/- 0.02 km. The equatorial radius is
   * 6378.136 km, and the polar radius is 6356.752 km. They differ by one part in 298.257.
   *
   * <p>Reference: http://ssd.jpl.nasa.gov/phys_props_earth.html, which quotes Yoder, C.F. 1995,
   * "Astrometric and Geodetic Properties of Earth and the Solar System" in Global Earth Physics, A
   * Handbook of Physical Constants, AGU Reference Shelf 1, American Geophysical Union, Table 2.
   */
  public static double getRadiusMeters() {
    return 6371010.0;
  }

  /** Returns the Earth's mean radius as above, but in kilometers. */
  public static double getRadiusKm() {
    return 0.001 * getRadiusMeters();
  }

}
