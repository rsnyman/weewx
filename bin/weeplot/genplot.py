#
#    Copyright (c) 2009-2023 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
"""Routines for generating image plots."""
from __future__ import absolute_import

import colorsys
import locale
import os
import time

try:
    from PIL import Image, ImageDraw
except ImportError:
    import Image, ImageDraw

from six.moves import zip

import weeplot.utilities
from weeplot.utilities import tobgr
import weeutil.weeutil
from weeutil.weeutil import max_with_none, min_with_none, to_bool, to_text


# NB: PIL (and most fonts) expect text strings to be in Unicode. Hence, any place where a label can
# be set should be protected by a call to weeutil.weeutil.to_text() to make sure the label is in
# Unicode.

class GeneralPlot(object):
    """Holds various parameters necessary for a plot. It should be specialized by the type of plot.
    """
    def __init__(self, plot_dict):
        """Initialize an instance of GeneralPlot.
        
        plot_dict: an instance of ConfigObj, or something that looks like it.
        """

        self.line_list = []
        
        self.xscale = (None, None, None)
        self.yscale = (None, None, None)

        self.anti_alias             = int(plot_dict.get('anti_alias', 1))

        self.image_width            = int(plot_dict.get('image_width', 300)) * self.anti_alias
        self.image_height           = int(plot_dict.get('image_height', 180)) * self.anti_alias
        self.image_background_color = tobgr(plot_dict.get('image_background_color', '0xf5f5f5'))

        self.chart_background_color = tobgr(plot_dict.get('chart_background_color', '0xd8d8d8'))
        self.chart_gridline_color   = tobgr(plot_dict.get('chart_gridline_color', '0xa0a0a0'))
        color_list                  = plot_dict.get('chart_line_colors', ['0xff0000', '0x00ff00', '0x0000ff'])
        fill_color_list             = plot_dict.get('chart_fill_colors', color_list)
        width_list                  = plot_dict.get('chart_line_width', [1, 1, 1])
        self.chart_line_colors      = [tobgr(v) for v in color_list]
        self.chart_fill_colors      = [tobgr(v) for v in fill_color_list]
        self.chart_line_widths      = [int(v) for v in width_list]

        
        self.top_label_font_path    = plot_dict.get('top_label_font_path')
        self.top_label_font_size    = int(plot_dict.get('top_label_font_size', 10)) * self.anti_alias

        self.unit_label             = None
        self.unit_label_font_path   = plot_dict.get('unit_label_font_path')
        self.unit_label_font_color  = tobgr(plot_dict.get('unit_label_font_color', '0x000000'))
        self.unit_label_font_size   = int(plot_dict.get('unit_label_font_size', 10)) * self.anti_alias
        self.unit_label_position    = (10 * self.anti_alias, 0)

        self.bottom_label           = u""
        self.bottom_label_font_path = plot_dict.get('bottom_label_font_path')
        self.bottom_label_font_color= tobgr(plot_dict.get('bottom_label_font_color', '0x000000'))
        self.bottom_label_font_size = int(plot_dict.get('bottom_label_font_size', 10)) * self.anti_alias
        self.bottom_label_offset    = int(plot_dict.get('bottom_label_offset', 3))

        self.axis_label_font_path   = plot_dict.get('axis_label_font_path')
        self.axis_label_font_color  = tobgr(plot_dict.get('axis_label_font_color', '0x000000'))
        self.axis_label_font_size   = int(plot_dict.get('axis_label_font_size', 10)) * self.anti_alias

        # Make sure the formats used for the x- and y-axes are in unicode.
        self.x_label_format         = to_text(plot_dict.get('x_label_format'))
        self.y_label_format         = to_text(plot_dict.get('y_label_format'))
        
        self.x_nticks               = int(plot_dict.get('x_nticks', 10))
        self.y_nticks               = int(plot_dict.get('y_nticks', 10))

        self.x_label_spacing        = int(plot_dict.get('x_label_spacing', 2))
        self.y_label_spacing        = int(plot_dict.get('y_label_spacing', 2))
        
        # Calculate sensible margins for the given image and font sizes.
        self.y_label_side = plot_dict.get('y_label_side', 'left')
        if self.y_label_side == 'left' or self.y_label_side == 'both':
            self.lmargin = int(4.0 * self.axis_label_font_size)
        else:
            self.lmargin = 20 * self.anti_alias
        if self.y_label_side == 'right' or self.y_label_side == 'both':
            self.rmargin = int(4.0 * self.axis_label_font_size)
        else:
            self.rmargin = 20 * self.anti_alias
        self.bmargin = int(1.5 * (self.bottom_label_font_size + self.axis_label_font_size) + 0.5)
        self.tmargin = int(1.5 * self.top_label_font_size + 0.5)
        self.tbandht = int(1.2 * self.top_label_font_size + 0.5)
        self.padding =  3 * self.anti_alias

        self.render_rose            = False
        self.rose_width             = int(plot_dict.get('rose_width', 21))
        self.rose_height            = int(plot_dict.get('rose_height', 21))
        self.rose_diameter          = int(plot_dict.get('rose_diameter', 10))
        self.rose_position          = (self.lmargin + self.padding + 5, self.image_height - self.bmargin - self.padding - self.rose_height)
        self.rose_rotation          = None
        self.rose_label             = to_text(plot_dict.get('rose_label', u'N'))
        self.rose_label_font_path   = plot_dict.get('rose_label_font_path', self.bottom_label_font_path)
        self.rose_label_font_size   = int(plot_dict.get('rose_label_font_size', 10))
        self.rose_label_font_color  = tobgr(plot_dict.get('rose_label_font_color', '0x000000'))
        self.rose_line_width        = int(plot_dict.get('rose_line_width', 1))
        self.rose_color             = plot_dict.get('rose_color')
        if self.rose_color is not None:
            self.rose_color = tobgr(self.rose_color)

        # Show day/night transitions
        self.show_daynight          = to_bool(plot_dict.get('show_daynight', False))
        self.daynight_day_color     = tobgr(plot_dict.get('daynight_day_color', '0xffffff'))
        self.daynight_night_color   = tobgr(plot_dict.get('daynight_night_color', '0xf0f0f0'))
        self.daynight_edge_color    = tobgr(plot_dict.get('daynight_edge_color', '0xefefef'))
        self.daynight_gradient      = int(plot_dict.get('daynight_gradient', 20))

        # initialize the location
        self.latitude               = None
        self.longitude              = None

        # normalize the font paths relative to the skin directory
        skin_dir = plot_dict.get('skin_dir', '')
        self.top_label_font_path = self.normalize_path(skin_dir, self.top_label_font_path)
        self.bottom_label_font_path = self.normalize_path(skin_dir, self.bottom_label_font_path)
        self.unit_label_font_path = self.normalize_path(skin_dir, self.unit_label_font_path)
        self.axis_label_font_path = self.normalize_path(skin_dir, self.axis_label_font_path)
        self.rose_label_font_path = self.normalize_path(skin_dir, self.rose_label_font_path)

    @staticmethod
    def normalize_path(skin_dir, path):
        if path is None:
            return None
        return os.path.join(skin_dir, path)

    def setBottomLabel(self, bottom_label):
        """Set the label to be put at the bottom of the plot. """
        # Make sure the label is in unicode or is None
        self.bottom_label = to_text(bottom_label)
        
    def setUnitLabel(self, unit_label):
        """Set the label to be used to show the units of the plot. """
        # Make sure the label is in unicode
        self.unit_label = to_text(unit_label)
        
    def setXScaling(self, xscale):
        """Set the X scaling.
        
        xscale: A 3-way tuple (xmin, xmax, xinc)
        """
        self.xscale = xscale
        
    def setYScaling(self, yscale):
        """Set the Y scaling.
        
        yscale: A 3-way tuple (ymin, ymax, yinc)
        """
        self.yscale = yscale
        
    def addLine(self, line):
        """Add a line to be plotted.
        
        line: an instance of PlotLine
        """
        if None in line.x:
            raise weeplot.ViolatedPrecondition("X vector cannot have any values 'None' ")
        self.line_list.append(line)

    def setLocation(self, lat, lon):
        self.latitude  = lat
        self.longitude = lon
        
    def setDayNight(self, showdaynight, daycolor, nightcolor, edgecolor):
        """Configure day/night bands.

        showdaynight: Boolean flag indicating whether to draw day/night bands

        daycolor: color for day bands

        nightcolor: color for night bands

        edgecolor: color for transition between day and night
        """
        self.show_daynight = showdaynight
        self.daynight_day_color = daycolor
        self.daynight_night_color = nightcolor
        self.daynight_edge_color = edgecolor

    def render(self):
        """Traverses the universe of things that have to be plotted in this image, rendering
        them and returning the results as a new Image object.
        """

        # NB: In what follows the variable 'draw' is an instance of an ImageDraw object and is in pixel units.
        # The variable 'sdraw' is an instance of ScaledDraw and its units are in the "scaled" units of the plot
        # (e.g., the horizontal scaling might be for seconds, the vertical for degrees Fahrenheit.)
        image = Image.new("RGB", (self.image_width, self.image_height), self.image_background_color)
        draw = self._getImageDraw(image)
        draw.rectangle(((self.lmargin,self.tmargin), 
                        (self.image_width - self.rmargin, self.image_height - self.bmargin)), 
                        fill=self.chart_background_color)
        
        self._renderBottom(draw)
        self._renderTopBand(draw)
        
        self._calcXScaling()
        self._calcYScaling()
        self._calcXLabelFormat()
        self._calcYLabelFormat()
        
        sdraw = self._getScaledDraw(draw)
        if self.show_daynight:
            self._renderDayNight(sdraw)
        self._renderXAxes(sdraw)
        self._renderYAxes(sdraw)
        self._renderPlotLines(sdraw)
        if self.render_rose:
            self._renderRose(image, draw)

        if self.anti_alias != 1:
            image.thumbnail((self.image_width / self.anti_alias,
                             self.image_height / self.anti_alias),
                            Image.LANCZOS)

        return image

    # noinspection PyMethodMayBeStatic
    def _getImageDraw(self, image):
        """Returns an instance of ImageDraw with the proper dimensions and background color"""
        draw = UniDraw(image)
        return draw
    
    def _getScaledDraw(self, draw):
        """Returns an instance of ScaledDraw, with the appropriate scaling.
        
        draw: An instance of ImageDraw
        """
        sdraw = weeplot.utilities.ScaledDraw(
            draw,
            (
                (self.lmargin + self.padding, self.tmargin + self.padding),
                (self.image_width - self.rmargin - self.padding, self.image_height - self.bmargin - self.padding)
            ),
            (
                (self.xscale[0], self.yscale[0]),
                (self.xscale[1], self.yscale[1])
            )
        )
        return sdraw
        
    def _renderDayNight(self, sdraw):
        """Draw vertical bands for day/night."""
        (first, transitions) = weeutil.weeutil.getDayNightTransitions(
            self.xscale[0], self.xscale[1], self.latitude, self.longitude)
        color = self.daynight_day_color \
            if first == 'day' else self.daynight_night_color
        xleft = self.xscale[0]
        for x in transitions:
            sdraw.rectangle(((xleft,self.yscale[0]),
                             (x,self.yscale[1])), fill=color)
            xleft = x
            color = self.daynight_night_color \
                if color == self.daynight_day_color else self.daynight_day_color
        sdraw.rectangle(((xleft,self.yscale[0]),
                         (self.xscale[1],self.yscale[1])), fill=color)
        if self.daynight_gradient:
            if first == 'day':
                color1 = self.daynight_day_color
                color2 = self.daynight_night_color
            else:
                color1 = self.daynight_night_color
                color2 = self.daynight_day_color
            nfade = self.daynight_gradient
            # gradient is longer at the poles than the equator
            d = 120 + 300 * (1 - (90.0 - abs(self.latitude)) / 90.0)
            for i in range(len(transitions)):
                last_ = self.xscale[0] if i == 0 else transitions[i-1]
                next_ = transitions[i+1] if i < len(transitions)-1 else self.xscale[1]
                for z in range(1,nfade):
                    c = blend_hls(color2, color1, float(z)/float(nfade))
                    rgbc = int2rgbstr(c)
                    x1 = transitions[i]-d*(nfade+1)/2+d*z
                    if last_ < x1 < next_:
                        sdraw.rectangle(((x1, self.yscale[0]),
                                         (x1+d, self.yscale[1])),
                                        fill=rgbc)
                if color1 == self.daynight_day_color:
                    color1 = self.daynight_night_color
                    color2 = self.daynight_day_color
                else:
                    color1 = self.daynight_day_color
                    color2 = self.daynight_night_color
        # draw a line at the actual sunrise/sunset
        for x in transitions:
            sdraw.line((x,x),(self.yscale[0],self.yscale[1]),
                       fill=self.daynight_edge_color)

    def _renderXAxes(self, sdraw):
        """Draws the x axis and vertical constant-x lines, as well as the labels. """

        axis_label_font = weeplot.utilities.get_font_handle(self.axis_label_font_path,
                                                            self.axis_label_font_size)

        drawlabelcount = 0
        for x in weeutil.weeutil.stampgen(self.xscale[0], self.xscale[1], self.xscale[2]) :
            sdraw.line((x, x),
                       (self.yscale[0], self.yscale[1]),
                       fill=self.chart_gridline_color,
                       width=self.anti_alias)
            if drawlabelcount % self.x_label_spacing == 0 :
                xlabel = self._genXLabel(x)
                axis_label_width = sdraw.draw.textlength(xlabel, font=axis_label_font)
                xpos = sdraw.xtranslate(x)
                sdraw.draw.text((xpos - axis_label_width/2, self.image_height - self.bmargin + 2),
                                xlabel, fill=self.axis_label_font_color, font=axis_label_font)
            drawlabelcount += 1

    def _renderYAxes(self, sdraw):
        """Draws the y axis and horizontal constant-y lines, as well as the labels.
        Should be sufficient for most purposes.
        """
        nygridlines     = int((self.yscale[1] - self.yscale[0]) / self.yscale[2] + 1.5)
        axis_label_font = weeplot.utilities.get_font_handle(self.axis_label_font_path,
                                                            self.axis_label_font_size)
        
        # Draw the (constant y) grid lines 
        for i in range(nygridlines) :
            y = self.yscale[0] + i * self.yscale[2]
            sdraw.line((self.xscale[0], self.xscale[1]), (y, y), fill=self.chart_gridline_color,
                       width=self.anti_alias)
            # Draw a label on every other line:
            if i % self.y_label_spacing == 0 :
                ylabel = self._genYLabel(y)
                left, top, right, bottom = axis_label_font.getbbox(ylabel)
                axis_label_width, axis_label_height = right - left, bottom - top
                ypos = sdraw.ytranslate(y)
                # We want to treat Truetype and bitmapped fonts the same. By default, Truetype
                # measures the top of the bounding box at the top of the ascender, while it's
                # the top of the text for bitmapped. Specify an anchor of "lt" (left, top) for
                # both.
                if self.y_label_side == 'left' or self.y_label_side == 'both':
                    sdraw.draw.text((self.lmargin - axis_label_width - 2, ypos - axis_label_height/2),
                                    ylabel, fill=self.axis_label_font_color, font=axis_label_font,
                                    anchor="lt")
                if self.y_label_side == 'right' or self.y_label_side == 'both':
                    sdraw.draw.text((self.image_width - self.rmargin + 4, ypos - axis_label_height/2),
                                    ylabel, fill=self.axis_label_font_color, font=axis_label_font,
                                    anchor="lt")

    def _renderPlotLines(self, sdraw):
        """Draw the collection of lines, using a different color for each one. Because there is
        a limited set of colors, they need to be recycled if there are very many lines.
        """
        nlines = len(self.line_list)
        ncolors = len(self.chart_line_colors)
        nfcolors = len(self.chart_fill_colors)
        nwidths = len(self.chart_line_widths)

        # Draw them in reverse order, so the first line comes out on top of the image
        for j, this_line in enumerate(self.line_list[::-1]):
            
            iline=nlines-j-1
            color = self.chart_line_colors[iline%ncolors] if this_line.color is None else this_line.color
            fill_color = self.chart_fill_colors[iline%nfcolors] if this_line.fill_color is None else this_line.fill_color
            width = (self.chart_line_widths[iline%nwidths] if this_line.width is None else this_line.width) * self.anti_alias

            # Calculate the size of a gap in data
            maxdx = None
            if this_line.line_gap_fraction is not None:
                maxdx = this_line.line_gap_fraction * (self.xscale[1] - self.xscale[0])

            if this_line.plot_type == 'line':
                ms = this_line.marker_size
                if ms is not None:
                    ms *= self.anti_alias
                sdraw.line(this_line.x, 
                           this_line.y, 
                           line_type=this_line.line_type,
                           marker_type=this_line.marker_type,
                           marker_size=ms,
                           fill  = color,
                           width = width,
                           maxdx = maxdx)
            elif this_line.plot_type == 'bar' :
                for x, y, bar_width in zip(this_line.x, this_line.y, this_line.bar_width):
                    if y is None:
                        continue
                    sdraw.rectangle(((x - bar_width, self.yscale[0]), (x, y)), fill=fill_color, outline=color)
            elif this_line.plot_type == 'vector' :
                for (x, vec) in zip(this_line.x, this_line.y):
                    sdraw.vector(x, vec,
                                 vector_rotate = this_line.vector_rotate,
                                 fill  = color,
                                 width = width)
                self.render_rose = True
                self.rose_rotation = this_line.vector_rotate
                if self.rose_color is None:
                    self.rose_color = color

    def _renderBottom(self, draw):
        """Draw anything at the bottom (just some text right now). """
        bottom_label_font = weeplot.utilities.get_font_handle(self.bottom_label_font_path,
                                                              self.bottom_label_font_size)
        left, top, right, bottom = bottom_label_font.getbbox(self.bottom_label)
        bottom_label_width, bottom_label_height = right - left, bottom - top

        draw.text(((self.image_width - bottom_label_width)/2,
                   self.image_height - bottom_label_height - self.bottom_label_offset),
                  self.bottom_label, 
                  fill=self.bottom_label_font_color,
                  font=bottom_label_font,
                  anchor="lt")
        
    def _renderTopBand(self, draw):
        """Draw the top band and any text in it. """
        # Draw the top band rectangle
        draw.rectangle(((0,0), 
                        (self.image_width, self.tbandht)), 
                        fill = self.chart_background_color)

        # Put the units in the upper left corner
        unit_label_font = weeplot.utilities.get_font_handle(self.unit_label_font_path,
                                                            self.unit_label_font_size)
        if self.unit_label:
            if self.y_label_side == 'left' or self.y_label_side == 'both':
                draw.text(self.unit_label_position,
                          self.unit_label,
                          fill=self.unit_label_font_color,
                          font=unit_label_font)
            if self.y_label_side == 'right' or self.y_label_side == 'both':
                unit_label_position_right = (self.image_width - self.rmargin + 4, 0)
                draw.text(unit_label_position_right,
                          self.unit_label,
                          fill=self.unit_label_font_color,
                          font=unit_label_font)

        top_label_font = weeplot.utilities.get_font_handle(self.top_label_font_path,
                                                           self.top_label_font_size)
        
        # The top label is the appended label_list. However, it has to be drawn in segments 
        # because each label may be in a different color. For now, append them together to get
        # the total width
        top_label = u' '.join([line.label for line in self.line_list])
        top_label_width= draw.textlength(top_label, font=top_label_font)
        
        x = (self.image_width - top_label_width)/2
        y = 0
        
        ncolors = len(self.chart_line_colors)
        for i, this_line in enumerate(self.line_list):
            color = self.chart_line_colors[i%ncolors] if this_line.color is None else this_line.color
            # Draw a label
            draw.text( (x,y), this_line.label, fill = color, font = top_label_font)
            # Now advance the width of the label we just drew, plus a space:
            label_width = draw.textlength(this_line.label + u' ', font= top_label_font)
            x += label_width

    def _renderRose(self, image, draw):
        """Draw a compass rose."""
        
        rose_center_x = self.rose_width/2  + 1
        rose_center_y = self.rose_height/2 + 1
        barb_width  = 3
        barb_height = 3
        # The background is all white with a zero alpha (totally transparent)
        rose_image = Image.new("RGBA", (self.rose_width, self.rose_height), (0x00, 0x00, 0x00, 0x00))
        rose_draw = ImageDraw.Draw(rose_image)
 
        fill_color = add_alpha(self.rose_color)
        # Draw the arrow straight up (North). First the shaft:
        rose_draw.line( ((rose_center_x, 0), (rose_center_x, self.rose_height)),
                        width = self.rose_line_width,
                        fill = fill_color)
        # Now the left barb:
        rose_draw.line( ((rose_center_x - barb_width, barb_height), (rose_center_x, 0)),
                        width = self.rose_line_width,
                        fill = fill_color)
        # And the right barb:
        rose_draw.line( ((rose_center_x, 0), (rose_center_x + barb_width, barb_height)),
                        width = self.rose_line_width,
                        fill = fill_color)
        
        rose_draw.ellipse(((rose_center_x - self.rose_diameter/2,
                            rose_center_y - self.rose_diameter/2),
                           (rose_center_x + self.rose_diameter/2,
                            rose_center_y + self.rose_diameter/2)),
                          outline = fill_color)

        # Rotate if necessary:
        if self.rose_rotation:
            rose_image = rose_image.rotate(self.rose_rotation)
            rose_draw = ImageDraw.Draw(rose_image)
        
        # Calculate the position of the "N" label:
        rose_label_font = weeplot.utilities.get_font_handle(self.rose_label_font_path,
                                                            self.rose_label_font_size)
        left, top, right, bottom = rose_label_font.getbbox(self.rose_label)
        rose_label_width, rose_label_height = right - left, bottom - top

        # Draw the label in the middle of the (possibly) rotated arrow
        rose_draw.text((rose_center_x - rose_label_width/2 - 1,
                        rose_center_y - rose_label_height/2 - 1),
                       self.rose_label,
                       fill=add_alpha(self.rose_label_font_color),
                       font=rose_label_font,
                       anchor="lt")

        # Paste the image of the arrow on to the main plot. The alpha
        # channel of the image will be used as the mask.
        # This will cause the arrow to overlay the background plot
        image.paste(rose_image, self.rose_position, rose_image)
        
    def _calcXScaling(self):
        """Calculates the x scaling. It will probably be specialized by
        plots where the x-axis represents time.
        """
        (xmin, xmax) = self._calcXMinMax()

        self.xscale = weeplot.utilities.scale(xmin, xmax, self.xscale, nsteps=self.x_nticks)
            
    def _calcYScaling(self):
        """Calculates y scaling. Can be used 'as-is' for most purposes."""
        # The filter is necessary because unfortunately the value 'None' is not
        # excluded from min and max (i.e., min(None, x) is not necessarily x). 
        # The try block is necessary because min of an empty list throws a
        # ValueError exception.
        ymin = ymax = None
        for line in self.line_list:
            if line.plot_type == 'vector':
                try:
                    # For progressive vector plots, we want the magnitude of the complex vector
                    yline_max = max(abs(c) for c in [v for v in line.y if v is not None])
                except ValueError:
                    yline_max = None
                yline_min = - yline_max if yline_max is not None else None
            else:
                yline_min = min_with_none(line.y)
                yline_max = max_with_none(line.y)
            ymin = min_with_none([ymin, yline_min])
            ymax = max_with_none([ymax, yline_max])

        if ymin is None and ymax is None :
            # No valid data. Pick an arbitrary scaling
            self.yscale=(0.0, 1.0, 0.2)
        else:
            self.yscale = weeplot.utilities.scale(ymin, ymax, self.yscale, nsteps=self.y_nticks)

    def _calcXLabelFormat(self):
        if self.x_label_format is None:
            self.x_label_format = weeplot.utilities.pickLabelFormat(self.xscale[2])

    def _calcYLabelFormat(self):
        if self.y_label_format is None:
            self.y_label_format = weeplot.utilities.pickLabelFormat(self.yscale[2])
        
    def _genXLabel(self, x):
        xlabel = locale.format_string(self.x_label_format, x)
        return xlabel
    
    def _genYLabel(self, y):
        ylabel = locale.format_string(self.y_label_format, y)
        return ylabel
    
    def _calcXMinMax(self):
        xmin = xmax = None
        for line in self.line_list:
            xline_min = min_with_none(line.x)
            xline_max = max_with_none(line.x)
            # If the line represents a bar chart, then the actual minimum has to
            # be adjusted for the bar width of the first point
            if line.plot_type == 'bar':
                xline_min = xline_min - line.bar_width[0]
            xmin = min_with_none([xmin, xline_min])
            xmax = max_with_none([xmax, xline_max])
        return xmin, xmax


class TimePlot(GeneralPlot) :
    """Class that specializes GeneralPlot for plots where the x-axis is time."""
    
    def _calcXScaling(self):
        """Specialized version for time plots."""
        if None in self.xscale:
            (xmin, xmax) = self._calcXMinMax()
            self.xscale = weeplot.utilities.scaletime(xmin, xmax)

    def _calcXLabelFormat(self):
        """Specialized version for time plots. Assumes that time is in unix epoch time."""
        if self.x_label_format is None:
            (xmin, xmax) = self._calcXMinMax()
            if xmin is not None and xmax is not None:
                delta = xmax - xmin
                if delta > 30*24*3600:
                    self.x_label_format = u"%x"
                elif delta > 24*3600:
                    self.x_label_format = u"%x %X"
                else:
                    self.x_label_format = u"%X"
        
    def _genXLabel(self, x):
        """Specialized version for time plots. Assumes that time is in unix epoch time."""
        if self.x_label_format is None:
            return u''
        time_tuple = time.localtime(x)
        # There are still some strftimes out there that don't support Unicode.
        try:
            xlabel = time.strftime(self.x_label_format, time_tuple)
        except UnicodeEncodeError:
            # Convert it to UTF8, then back again:
            xlabel = time.strftime(self.x_label_format.encode('utf-8'), time_tuple).decode('utf-8')
        return xlabel


class PlotLine(object):
    """Represents a single line (or bar) in a plot. """
    def __init__(self, x, y, label='', color=None, fill_color=None, width=None, plot_type='line',
                 line_type='solid', marker_type=None, marker_size=10, 
                 bar_width=None, vector_rotate = None, line_gap_fraction=None):
        self.x               = x
        self.y               = y
        self.label           = to_text(label)   # Make sure the label is in unicode
        self.plot_type       = plot_type
        self.line_type       = line_type
        self.marker_type     = marker_type
        self.marker_size     = marker_size
        self.color           = color
        self.fill_color      = fill_color
        self.width           = width
        self.bar_width       = bar_width
        self.vector_rotate   = vector_rotate
        self.line_gap_fraction = line_gap_fraction


class UniDraw(ImageDraw.ImageDraw):
    """Supports non-Unicode fonts
    
    Not all fonts support Unicode characters. These will raise a UnicodeEncodeError exception.
    This class subclasses the regular ImageDraw.Draw class, adding overridden functions to
    catch these exceptions. It then tries drawing the string again, this time as a UTF8 string
    """
    
    def text(self, position, string, **options):
        try:
            return ImageDraw.ImageDraw.text(self, position, string, **options)
        except UnicodeEncodeError:
            return ImageDraw.ImageDraw.text(self, position, string.encode('utf-8'), **options)


def blend_hls(c, bg, alpha):
    """Fade from c to bg using alpha channel where 1 is solid and 0 is
    transparent.  This fades across the hue, saturation, and lightness."""
    return blend(c, bg, alpha, alpha, alpha)


def blend_ls(c, bg, alpha):
    """Fade from c to bg where 1 is solid and 0 is transparent.
    Change only the lightness and saturation, not hue."""
    return blend(c, bg, 1.0, alpha, alpha)


def blend(c, bg, alpha_h, alpha_l, alpha_s):
    """Fade from c to bg in the hue, lightness, saturation colorspace.
       Added hue directionality to choose shortest circular hue path e.g.
       https://stackoverflow.com/questions/1416560/hsl-interpolation
       Also, grey detection to minimize colour wheel travel.  Interesting resource:
       http://davidjohnstone.net/pages/lch-lab-colour-gradient-picker
       """

    r1,g1,b1 = int2rgb(c)
    h1,l1,s1 = colorsys.rgb_to_hls(r1/255.0, g1/255.0, b1/255.0)

    r2,g2,b2 = int2rgb(bg)
    h2,l2,s2 = colorsys.rgb_to_hls(r2/255.0, g2/255.0, b2/255.0)

    # Check if either of the values is grey (saturation 0),
    # in which case don't needlessly reset hue to '0', reducing travel around colour wheel
    if s1 == 0: h1 = h2
    if s2 == 0: h2 = h1

    h_delta = h2 - h1

    if abs(h_delta) > 0.5:
        # If interpolating over more than half-circle (0.5 radians) take shorter, opposite direction...
        h_range = 1.0 - abs(h_delta)
        h_dir = +1.0 if h_delta < 0.0 else -1.0

        # Calculte h based on line back from h2 as proportion of h_range and alpha
        h = h2 - ( h_dir * h_range * alpha_h )

        # Clamp h within 0.0 to 1.0 range
        h = h + 1.0 if h < 0.0 else h
        h = h - 1.0 if h > 1.0 else h
    else:
        # Interpolating over less than a half-circle, so use normal interpolation as before
        h = alpha_h * h1 + (1 - alpha_h) * h2

    l = alpha_l * l1 + (1 - alpha_l) * l2
    s = alpha_s * s1 + (1 - alpha_s) * s2

    r,g,b = colorsys.hls_to_rgb(h, l, s)

    r = round(r * 255.0)
    g = round(g * 255.0)
    b = round(b * 255.0)

    t = rgb2int(int(r),int(g),int(b))

    return int(t)


def int2rgb(x):
    b = (x >> 16) & 0xff
    g = (x >> 8) & 0xff
    r = x & 0xff
    return r,g,b


def int2rgbstr(x):
    return '#%02x%02x%02x' % int2rgb(x)


def rgb2int(r,g,b):
    return r + g*256 + b*256*256


def add_alpha(i):
    """Add an opaque alpha channel to an integer RGB value"""
    r = i & 0xff
    g = (i >> 8)  & 0xff
    b = (i >> 16) & 0xff
    a = 0xff    # Opaque alpha
    return r,g,b,a
