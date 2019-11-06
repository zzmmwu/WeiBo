
function RandomString(strlen: number): string{
    let template = "abcdefghijklmnopqrstuvwxyz1234567890";
    let str = "";
    for(let i=0; i<strlen; ++i){
        let randIndex = Math.floor(Math.random()*template.length);
        str += template.substr(randIndex, 1);
    }
    return str;
}

function fromAsciiNumCharToString(asciiStr: string): string{
    let str = "";
    let tempStr = "";
    for(let i=0; i<asciiStr.length; ++i){
        if(asciiStr[i] != ","){
            tempStr += asciiStr[i];
        }else{
            let num = Number(tempStr);
            str += String.fromCharCode(num);
            tempStr = "";
        }
    }
    if(tempStr != ""){
        let num = Number(tempStr);
        str += String.fromCharCode(num);
        tempStr = "";
    }


    return str;
}
function fromAsciiNumArrToString(asciiArr: Uint8Array): string{
    
    let str = "";
    for(let i=0; i<asciiArr.length; ++i){
        let num = Number(asciiArr[i]);
        str += String.fromCharCode(num);
    }

    return str;
}
function fromStringToAsciiNumArr(str: string): Uint8Array{
    
    let arr = new Uint8Array(str.length);
    for(let i=0; i<str.length; ++i){
        arr[i] = str.charCodeAt(i);
    }
    return arr;
}

//v1大于v2返回1，相同返回0， 小于返回--1
function compareVersion(v1, v2) {
  v1 = v1.split('.')
  v2 = v2.split('.')
  const len = Math.max(v1.length, v2.length)

  while (v1.length < len) {
    v1.push('0')
  }
  while (v2.length < len) {
    v2.push('0')
  }

  for (let i = 0; i < len; i++) {
    const num1 = parseInt(v1[i])
    const num2 = parseInt(v2[i])

    if (num1 > num2) {
      return 1
    } else if (num1 < num2) {
      return -1
    }
  }

  return 0
}