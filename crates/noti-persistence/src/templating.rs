//! Tera-based template engine implementing `TemplateEngineTrait`.

use noti_core::error::{NotiError, Result};
use noti_core::traits::TemplateEngineTrait;
use serde_json::Value;
use tera::{Context, Tera};

pub struct TemplateEngine {
    tera: Tera,
}

impl TemplateEngine {
    /// # Errors
    ///
    /// Returns an error if the Tera engine cannot be initialized from the
    /// given templates directory path.
    pub fn new(templates_path: &str) -> std::result::Result<Self, anyhow::Error> {
        let mut tera = Tera::new(&format!("{templates_path}/**/*"))
            .map_err(|e| anyhow::anyhow!("Failed to initialize Tera engine: {e}"))?;

        tera.autoescape_on(vec!["html"]);

        Ok(Self { tera })
    }

    /// Register a string-based template dynamically.
    ///
    /// # Errors
    ///
    /// Returns an error if the template name is duplicate or the content is
    /// invalid Tera syntax.
    pub fn add_raw_template(&mut self, name: &str, content: &str) -> Result<()> {
        self.tera
            .add_raw_template(name, content)
            .map_err(|e| NotiError::Template(e.to_string()))
    }
}

impl TemplateEngineTrait for TemplateEngine {
    fn render(&self, template_id: &str, variables: &Value) -> Result<String> {
        let context = Context::from_value(variables.clone())
            .map_err(|e| NotiError::Template(format!("context creation failed: {e}")))?;

        self.tera
            .render(template_id, &context)
            .map_err(|e| NotiError::Template(format!("render '{template_id}' failed: {e}")))
    }
}
