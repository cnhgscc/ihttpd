use jwt::{Token, Header, Claims};
use base64::prelude::*;
pub fn jwtsign(token: String) -> Claims {
    let t = Token::<Header, Claims, _>::parse_unverified(&*token).unwrap();
    let claims =  t.claims().clone();

    let download_path = claims.private.get("download_path").unwrap().to_string().trim_matches('"').to_string();

    let a = BASE64_STANDARD.decode(download_path);
    println!("{:?}", a.unwrap());
    claims
}
