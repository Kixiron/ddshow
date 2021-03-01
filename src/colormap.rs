use colorous::{Color as InnerColor, Gradient};
use std::{
    fmt::{self, Debug, Display},
    time::Duration,
};

pub const LUMINANCE_THRESHOLD: f32 = 0.5;

pub struct Color(InnerColor);

impl Color {
    pub const fn new(color: InnerColor) -> Self {
        Self(color)
    }

    /// Gets the text color that should be used for this color
    pub fn text_color(&self) -> Color {
        if self.luminance() >= LUMINANCE_THRESHOLD {
            // #0E1111
            Self(InnerColor {
                r: 14,
                g: 17,
                b: 17,
            })
        } else {
            // #EEEEEE
            Self(InnerColor {
                r: 238,
                g: 238,
                b: 238,
            })
        }
    }

    /// Get the luminance of a color
    // https://en.wikipedia.org/wiki/Relative_luminance
    pub fn luminance(&self) -> f32 {
        let Self(InnerColor { r, g, b }) = *self;
        let (r, g, b) = (r as f32 / 255.0, g as f32 / 255.0, b as f32 / 255.0);

        (0.2126 * r) + (0.7152 * g) + (0.0722 * b)
    }
}

impl Debug for Color {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(InnerColor { r, g, b }) = *self;
        write!(f, "\"#{:02X}{:02X}{:02X}\"", r, g, b)
    }
}

impl Display for Color {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(InnerColor { r, g, b }) = *self;
        write!(f, "#{:02X}{:02X}{:02X}", r, g, b)
    }
}

pub fn select_color(
    colormap: &Gradient,
    duration: Duration,
    (upper_bound, lower_bound): (Duration, Duration),
) -> Color {
    let lower_bound = lower_bound.as_secs_f64().log2();
    let upper_bound = upper_bound.as_secs_f64().log2();

    Color::new(colormap.eval_continuous(
        ((duration.as_secs_f64().log2() - lower_bound) / (upper_bound - lower_bound)) * 1.0,
    ))
}
